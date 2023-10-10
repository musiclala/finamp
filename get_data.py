import pandas as pd
import json
import requests


def get_data_from_wb(f_key: str, f_path_to_csv: str) -> str:
    """
    Собираем данные с wb.
    :param f_key: API ключ для wb
    :param f_path_to_csv: путь для сохранения csv
    :return: Просто возвращаю текст об успехе
    """
    url = f'https://statistics-api.wildberries.ru/api/v1/supplier/stocks?dateFrom=2023-10-01'

    headers = {
        'Authorization': f_key,
    }

    response = requests.get(url=url, headers=headers)
    data = json.loads(response.text)  # Преобразую данные в json
    df = pd.DataFrame(data)  # Преобразую данные в dataframe
    df.to_csv(f_path_to_csv, index=False)  # Преобразую данные в csv файл

    return 'Data received!'

