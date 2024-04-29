# YouTube data extraction project! 🚀🎥📊

### Project Overview: 
This project focuses on extracting YouTube data using Python, Airflow, and PostgreSQL. The workflow includes several key steps:
#### 1) Environment Setup:
Set up Docker containers for Airflow and MinIO. Airflow serves as the orchestrator for your data pipeline, while MinIO acts as the object storage system. I'm using postgresql server on dbeaver (cross-platform database tool)

<img src="https://github.com/BasitAli05/Youtube_Data_ETL_Pipeline/assets/106751594/133caef2-f10a-4f11-901d-909aaf919817" alt="Docker" width="900" height="450">

#### 2) Data Extraction DAG (Directed Acyclic Graph):
Created an Airflow DAG that orchestrates the extraction of YouTube data. This DAG likely includes tasks of extracting & transforming data.

<img src="https://github.com/BasitAli05/Youtube_Data_ETL_Pipeline/assets/106751594/6f788aa0-2f6e-4cb8-979a-07d70cc6a4f3" alt="Apache Airflow" width="900" height="450">

#### 3) CSV File Storage:
After data extraction and transformation, the resulting CSV file is stored locally or within the Airflow environment. This file contains the relevant YouTube data.

#### 4) MinIO Object Storage:
The CSV file is then uploaded to MinIO, an object storage system. MinIO provides scalable and secure storage for your data.

<img src="https://github.com/BasitAli05/Youtube_Data_ETL_Pipeline/assets/106751594/954cb84a-76a9-4590-a90b-08e62c7c1717" alt="MinIO" width="900" height="450">

#### 5) Data Migration to PostgreSQL:
Finally, migrate the data from MinIO to a PostgreSQL server. This step involves creating the necessary tables in PostgreSQL and loading the data.

<img src="https://github.com/BasitAli05/Youtube_Data_ETL_Pipeline/assets/106751594/347cf400-e337-4350-ad1b-304620584858" alt="DBeaver" width="900" height="450">

## Thank You
