import pandas as pd
import json
import requests


def get_data_from_wb(f_key: str, f_path_to_csv: str) -> str:
    url = f'https://statistics-api.wildberries.ru/api/v1/supplier/stocks?dateFrom=2023-10-01'

    headers = {
        'Authorization': f_key,
    }

    response = requests.get(url=url, headers=headers)
    data = json.loads(response.text)
    df = pd.DataFrame(data)
    df.to_csv(f_path_to_csv, index=False)

    return 'Data received!'

