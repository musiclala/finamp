from get_data import get_data_from_wb
from load_data import load_data, get_data_from_bq

from dotenv import load_dotenv
import os

load_dotenv()

key = os.getenv('KEY_WB')
path_to_csv = os.getenv('PATH_TO_CSV')
path_to_cred = os.getenv('PATH_TO_CRED')
project_id = os.getenv('PROJECT_ID')
dataset_id = os.getenv('DATASET')
table_id = os.getenv('TABLE')

if __name__ == '__main__':
    # print(get_data_from_wb(key, path_to_csv))
    # print(load_data(path_to_cred, project_id, dataset_id, table_id, path_to_csv))
    print(get_data_from_bq(path_to_cred, project_id))