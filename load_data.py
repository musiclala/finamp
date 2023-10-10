import pandas as pd
import pygsheets
from google.cloud import bigquery
from google.oauth2 import service_account

from dotenv import load_dotenv
import os

load_dotenv()

path_to_cred = os.getenv('PATH_TO_CRED')
project_id = os.getenv('PROJECT_ID')
dataset_id = os.getenv('DATASET')
table_id = os.getenv('TABLE')
path_to_csv = os.getenv('PATH_TO_CSV')


def load_data(f_path_to_cred: str,
              f_project_id: str,
              f_dataset_id: str,
              f_table_id: str,
              f_path_to_csv: str) -> str:
    credentials = service_account.Credentials.from_service_account_file(f_path_to_cred)
    client = bigquery.Client(credentials=credentials, project=f_project_id)

    dataset_ref = client.dataset(f_dataset_id)
    table_ref = dataset_ref.table(f_table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True

    with open(f_path_to_csv, "r+b") as file:
        job = client.load_table_from_file(
            file,
            table_ref,
            location="US",
            job_config=job_config,
        )

    job.result()

    return 'Data loaded!'


def get_data_from_bq(f_path_to_cred: str,
                     f_project_id: str):
    credentials = service_account.Credentials.from_service_account_file(f_path_to_cred)
    client = bigquery.Client(credentials=credentials, project=f_project_id)

    sql = """
    	   SELECT lastChangeDate,warehouseName,supplierArticle,nmId,barcode,quantity,inWayToClient,inWayFromClient,
    	   quantityFull,category,subject,brand,techSize,Price,Discount,isSupply,isRealization,SCCode
    	   FROM hours-analytics.test_dataset.stocks
    	   LIMIT 1000 """

    df2 = client.query(sql, project=f_project_id).to_dataframe()

    gc = pygsheets.authorize(service_file=f_path_to_cred)
    sheet = gc.sheet.get(os.getenv('SHEET_ID'))


    return 0
