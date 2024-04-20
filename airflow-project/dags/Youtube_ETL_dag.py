from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
import os
import googleapiclient.discovery
import pandas as pd
import emoji

default_args={
    'owner':'basit',
    'retries':0,
}

def extract_data_from_YT_API(ti):
    api_service_name = "youtube"
    api_version = "v3"
    api_key = "AIzaSyC_w48ekRLFZSfhll8Q0MLqcXlM9wWrpIs"  # set your API key

    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
    request = youtube.commentThreads().list(
        part="snippet, replies",
        videoId="TkqWaD-8Ys8"
    )
    response = request.execute()
    comments = []

    def extract_data(response_data):
        for item in response_data['items']:
            author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
            comment = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"]
            likeCount = item["snippet"]["topLevelComment"]["snippet"]["likeCount"]
            publishedAt = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
            comment_info = {'author': author, 'comment': comment, 'like_count': likeCount, 'published_at': publishedAt}
            comments.append(comment_info)

    while response.get('nextPageToken', None):
        request = youtube.commentThreads().list(
            part='id,replies,snippet',
            videoId="TkqWaD-8Ys8",
            pageToken=response['nextPageToken']
        )
        response = request.execute()
        extract_data(response)

    ti.xcom_push(key='data', value=comments)
    print(f'Finished processing {len(comments)} comments.')


def transform_data(ti):

    data = ti.xcom_pull(key='data', task_ids='extract_data_from_YT_API')
    df = pd.DataFrame(data)

    def remove_emojis(comment):
        return emoji.replace_emoji(comment, replace='')

    df['comment'] = df['comment'].apply(remove_emojis)
    df['comment'] = df['comment'].str.replace("'", "")
    # Removing @ from author name
    df['author'] = df['author'].str.replace('^@', '', regex=True)

    df['published_at'] = pd.to_datetime(df['published_at'])

    # Extract date and time into separate columns
    df['date'] = df['published_at'].dt.date
    df['time'] = df['published_at'].dt.time

    df.drop(columns=['published_at'], inplace=True)

    df['date'] = df['date'].astype(str)
    df['time'] = df['time'].astype(str)
    transformed_data = df

    path = 'dags/files/data.csv'
    transformed_data.to_csv(path, header=True, index=False)

def store_data():

    path = 'dags/files/data.csv'

    # Upload data file into s3
    s3_hook = S3Hook(aws_conn_id='minio_conn')
    s3_hook.load_file(
        filename=path,
        key=f'youtube/data.csv',
        bucket_name='airflow',
        replace=True
    )

    # Delete the file to empty disk space
    if os.path.exists(path):
        os.remove(path)

def insert_into_postgres():    

    # Fetch data from MinIO
    s3_hook = S3Hook(aws_conn_id='minio_conn')
    bucket_name = 'airflow'
    folder_name = 'youtube'
    file_name = 'data.csv'
    key = f'{folder_name}/{file_name}'

    # Get the contents of the object
    object_contents = s3_hook.get_key(key, bucket_name)

    # If the object exists, print its contents
    if object_contents:
        data = pd.read_csv(object_contents.get()['Body'])
    else:
        print(f"Object with key '{key}' does not exist in bucket '{bucket_name}'.")

    # Load it into postgresql database
    postgres_hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("TRUNCATE TABLE youtube_data;")

    for index, row in data.iterrows():
        # Initialize lists to store column names and values
        columns = []
        values = []

        # Iterate over each column-value pair in the row
        for column, value in row.items():
            # Add the column name to the list of columns
            columns.append(column)
            # Add the value to the list of values (properly formatted)
            if column == 'like_count':
                values.append(str(value))
            else:
                values.append(f"'{value}'")

        # SQL query for the current row
        sql_query = f"INSERT INTO youtube_data ({', '.join(columns)}) VALUES ({', '.join(values)})"
        cursor.execute(sql_query)

    # Commit the transaction
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()
    
with DAG(
    default_args=default_args,
    dag_id='Youtube_ETL_DAG_v01',
    description='ETL dag for extracting youtube comments data and loading into postgres database',
    start_date=datetime(2024, 4, 18),
    schedule_interval='@daily'
) as dag:
    task1=PythonOperator(
        task_id='extract_data_from_YT_API',
        python_callable=extract_data_from_YT_API
    )
    task2=PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )
    task3=PythonOperator(
        task_id='store_data',
        python_callable=store_data
    )
    task4=PostgresOperator(
        task_id='postgres_conn',
        postgres_conn_id='postgres_localhost',
        sql="""
            create table if not exists youtube_data(
                author character varying,
                comment character varying,
                like_count int,
                date date,
                time time
            )
        """
    )
    task5=PythonOperator(
        task_id='insert_into_postgres',
        python_callable=insert_into_postgres
    )

    task1 >> task2 >> task3 >> task4 >> task5