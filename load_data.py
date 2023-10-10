import pygsheets
from google.cloud import bigquery
from google.oauth2 import service_account

import os


def load_data(f_path_to_cred: str,
              f_project_id: str,
              f_dataset_id: str,
              f_table_id: str,
              f_path_to_csv: str) -> str:
    """
    Загружаем данные в BigQuery
    :param f_path_to_cred: путь до файла с credentials
    :param f_project_id: id проекта в BigQuery
    :param f_dataset_id: id датасета в BigQuery
    :param f_table_id: id таблицы в BigQuery
    :param f_path_to_csv: путь до csv-файла с данными
    :return: Просто возвращаем текст об успехе
    """
    # Коннектимся к BigQuery
    credentials = service_account.Credentials.from_service_account_file(f_path_to_cred)
    client = bigquery.Client(credentials=credentials, project=f_project_id)

    # Настраиваем BigQuery для работы
    dataset_ref = client.dataset(f_dataset_id)
    table_ref = dataset_ref.table(f_table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True

    # Работает с файлом и загружаем данные в базу
    with open(f_path_to_csv, "r+b") as file:
        job = client.load_table_from_file(
            file,
            table_ref,
            location="US",
            job_config=job_config,
        )

    job.result()

    return 'Data loaded!'


def get_data_from_bq_and_save_sheet(f_path_to_cred: str,
                                    f_project_id: str):
    """
    Собираем данные из BigQuery и сохраняем в таблицу.
    :param f_path_to_cred: Путь до файла с credentials
    :param f_project_id: id проекта в BigQuery
    :return: Просто возвращаем текст об успехе
    """
    # Коннектимся к BigQuery
    credentials = service_account.Credentials.from_service_account_file(f_path_to_cred)
    client = bigquery.Client(credentials=credentials, project=f_project_id)

    # Собираем данные из BigQuery
    sql = """
    	   SELECT lastChangeDate,warehouseName,supplierArticle,nmId,barcode,quantity,inWayToClient,inWayFromClient,
    	   quantityFull,category,subject,brand,techSize,Price,Discount,isSupply,isRealization,SCCode
    	   FROM hours-analytics.test_dataset.stocks
    	   LIMIT 1000 """

    # Сохраняем данные в dataframe
    df2 = client.query(sql, project=f_project_id).to_dataframe()

    # Получаем таблицу по id
    gc = pygsheets.authorize(service_file=f_path_to_cred)
    sh = gc.open_by_key(os.getenv('SHEET_ID'))

    try:
        sh.add_worksheet('Sheet1')
    except:
        pass
    # Заполняем таблицу из dataframe
    sheet = sh.worksheet_by_title('Sheet1')
    sheet.clear()
    sheet.set_dataframe(df2, (1, 1), encoding='utf-8', fit=True)

    return "Data saved to table!"
