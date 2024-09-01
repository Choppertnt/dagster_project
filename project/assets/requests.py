import os.path
from googleapiclient.discovery import build
import pandas as pd
from google.oauth2 import service_account
from dagster import asset,AssetIn,MetadataValue,MaterializeResult
from sqlalchemy import create_engine,text
import psycopg2
import seaborn as sns
import matplotlib.pyplot as plt
import base64
import plotly.io as pio
from dotenv import load_dotenv
from . import constants

load_dotenv()

@asset (required_resource_keys={"postgres_db"},group_name="CLOUD_API",compute_kind="python")
def connect_api():
    """The connect_api is API from VSCODE connect my google drive to pick csv file"""
    SCOPES = ['https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/spreadsheets']
    creds = service_account.Credentials.from_service_account_file(constants.API_TEST_FILES, scopes=SCOPES)
    service = build('drive', 'v3', credentials=creds)
    folder_id = os.getenv('FOLDERID')
    results = service.files().list(q="'{}' in parents".format(folder_id),
                                pageSize=10, fields="nextPageToken, files(id, name)").execute()
    items = results.get('files', [])
    if not items:
        print('No files found in the folder.')
    else:
        for item in items:
            if 'data2' in item['name'].lower():
                key_id2 = item['id']
                print('key_id2 :',item['id'],',',item['name'])
            elif 'data1' in item['name'].lower():
                key_id1 = item['id']
                print('key_id1 :',item['id'],',',item['name'])
            else:
                print(item['name'],'No file found with today\'s date.')
    return service,key_id1,key_id2


@asset(ins={"connect_api": AssetIn()},required_resource_keys={"postgres_db"},group_name="READ_Data_CSV",compute_kind="googlesheets")
def employee(connect_api):
    """This pick data employee from drive is file.csv"""
    service,key_id1,_ = connect_api
    file_id1 = key_id1
    results1 = service.files().list(q="'{}' in parents".format(file_id1),
                                pageSize=10, fields="nextPageToken, files(id, name)").execute()
    items1 = results1.get('files', [])
    if not items1:
        print('No files found in the folder.')
    else:
        excel_files1 = []
        for item1 in items1:
            if item1['name'].endswith('.csv'):
                excel_files1.append(item1)
            if not excel_files1:
                print('No Excel files found in the folder.')
            else:
                excel_file1 = excel_files1[0]
                file_id1 = excel_file1['id']
                file_download1 = service.files().get_media(fileId=file_id1).execute()
                with open('output/downloaded_file1.csv', 'wb') as f:
                    f.write(file_download1)
                df1 = pd.read_csv('output/downloaded_file1.csv')
    return df1


@asset(ins={"connect_api": AssetIn()},required_resource_keys={"postgres_db"},group_name="READ_Data_CSV",compute_kind="googlesheets")
def career(connect_api):
    """This pick data career from drive is file.csv"""
    service,_,key_id2 = connect_api
    file_id2 = key_id2
    results2 = service.files().list(q="'{}' in parents".format(file_id2),
                                pageSize=10, fields="nextPageToken, files(id, name)").execute()
    items2 = results2.get('files', [])
    if not items2:
        print('No files found in the folder.')
    else:
        excel_files2 = []
        for item2 in items2:
            if item2['name'].endswith('.csv'):
                excel_files2.append(item2)
            if not excel_files2:
                print('No Excel files found in the folder.')
            else:
                excel_file2 = excel_files2[0]
                file_id2 = excel_file2['id']
                file_download2 = service.files().get_media(fileId=file_id2).execute()
                with open('output/downloaded_file2.csv', 'wb') as f:
                    f.write(file_download2)
                df2 = pd.read_csv('output/downloaded_file2.csv')
    return df2



@asset(ins={"employee": AssetIn()},required_resource_keys={"postgres_db"},group_name="Upload_to_PostgreSQL",compute_kind="postgres")
def upload_employee(employee):
    """import employee to postgresql"""
    databb1 = employee
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    database = os.getenv('POSTGRES_DATABASE')
    host = os.getenv('POSTGRES_HOST')
    port = int(os.getenv('POSTGRES_PORT'))
    con = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    with con.connect() as connection:
        databb1.to_sql('data1', con=connection, if_exists='append', index=False)
        row_count = connection.execute(text("SELECT COUNT(*) FROM data1")).scalar()
    return MaterializeResult(
        metadata={
            'Number of records': MetadataValue.int(row_count)
        }
    )


@asset(ins={"career": AssetIn()},required_resource_keys={"postgres_db"},group_name="Upload_to_PostgreSQL",compute_kind="postgres")
def upload_career(career):
    """import career to postgresql"""
    databb2 = career
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    database = os.getenv('POSTGRES_DATABASE')
    host = os.getenv('POSTGRES_HOST')
    port = int(os.getenv('POSTGRES_PORT'))
    con = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    with con.connect() as connection:
        databb2.to_sql('data2', con=connection, if_exists='append', index=False)
        row_count = connection.execute(text("SELECT COUNT(*) FROM data2")).scalar()
    return MaterializeResult(
        metadata={
            'Number of records': MetadataValue.int(row_count)
        }
    ) 

@asset(deps=["upload_employee","upload_career"], required_resource_keys={"postgres_db"},group_name="Condition",compute_kind="matplotlib")
def sales_role():
    """export data from postgresql to plot bargraph of totalsales in role"""
    conn_params = {
        'dbname': os.getenv('POSTGRES_DATABASE'),'user': os.getenv('POSTGRES_USER'),'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('POSTGRES_HOST'),'port': int(os.getenv('POSTGRES_PORT'))}
    select_query = """select role,ROUND(SUM(cast(sales as numeric)), 2) as total_sales from (
    select * from data1
    left join data2 ON data1.id = data2.id)
    group by role order by role
    """
    try:
        with psycopg2.connect(**conn_params) as conn:
            total_sales = pd.read_sql(select_query, conn)
    except Exception as e:
        print(f"Error: {e}")
        return

    plt.figure(figsize=(17, 6))
    sns.barplot(x='role', y='total_sales', data=total_sales)
    plt.tight_layout()
    plt.title("Total Sales by Role")
    plt.savefig(constants.FILE_PATH_SAVE, dpi=300)
    with open(constants.FILE_PATH_SAVE, 'rb') as file:
        image_data = file.read()
    base64_data = base64.b64encode(image_data).decode('utf-8')
    md_content = f"![Image](data:image/jpeg;base64,{base64_data})"
    return MaterializeResult(
        metadata={
            "preview": MetadataValue.md(md_content)
        }
    )



@asset(
     deps=["customersales"],required_resource_keys={"postgres_db"},group_name="Condition",compute_kind="matplotlib"
)
def total_sales_career():
    """
         A chart of total sales by career
     """
    conn_params = {
        'dbname': 'postgres','user': 'postgres','password': '1160','host': 'localhost','port': 5432}
    select_query = """select role,sum as total_sales from customersales
    order by sum desc
    """
    try:
        with psycopg2.connect(**conn_params) as conn:
            total_sales = pd.read_sql(select_query, conn)
    except Exception as e:
        print(f"Error: {e}")
        return
    plt.figure(figsize=(5, 4))
    sns.barplot(x='role', y='total_sales', data=total_sales)
    plt.title("Total Sales by Role")
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig(constants.FILE_PATH_SAVE, dpi=300)
    with open(constants.FILE_PATH_SAVE, 'rb') as file:
        image_data = file.read()
    base64_data = base64.b64encode(image_data).decode('utf-8')
    md_content = f"![Image](data:image/jpeg;base64,{base64_data})"
    return MaterializeResult(
        metadata={
            "preview": MetadataValue.md(md_content)
        }
    )


