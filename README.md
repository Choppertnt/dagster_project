# Dagster , GoogleDrive , Postgresql , DBT

## Objective
This project demonstrates the use of Dagster for orchestration, Python for data cleaning and validation, data extraction from Google Drive, uploading data to PostgreSQL, and using DBT to transform the data.

## Features
- **Dagster 1.7.12** 
- **DBT 1.8.2** 
- **google.oauth2**
- **googleapiclient**

## Components
1. **Dagster**:
    - Used to schedule and monitor workflows.
    - Employed for workflow orchestration and task management.
2. **DBT**:
    - Used for transforming data to make it ready for use.
3. **Google drive**:
    - Used for data storage and management.
4. **PostgreSQL**:
    - Data is uploaded to PostgreSQL, where PostgreSQL serves as a data warehouse.

## Workflow
1. **Connect python and googledrive with API**:
    - Use a Service Account from Google Cloud Platform to access CSV files in Google Drive.
2. **Get data from CSV files in Google Drive**:
    - When Python can access Google Drive, retrieve data from CSV files using pandas and proceed to the next step.
    - The data consists of two files: the first is employee, and the second is career.
3. **Upload data to PostgreSQL**:
    - From the previous step, the next task is to upload the retrieved data into PostgreSQL. We'll take the values obtained earlier and upload them into the database.
    - Use function sqlalchemy 
4. **Use DBT for data transformation**:
    - Use DBT to transform data from a PostgreSQL data source.
5. **Plot a graph using data from the transformation**:
    - Plot a bar graph using the transformed data to visualize which profession has the highest sales.
