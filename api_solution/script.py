from requests import get
from dotenv import load_dotenv
from os import environ as env

from utils.db_connect import cursor, connection
from utils.utils import timer
from utils.models import create_models

load_dotenv()


def write_data(data: dict, okved: str) -> None:
    result = []
    for data in data:
        result.append((data["НаимПолн"], okved, data["ИНН"], data["КПП"], data["ЮрАдрес"]))

    cursor.executemany("""insert into okved_62_org_info(
                                        company_name,
                                        okved,
                                        inn,
                                        kpp,
                                        registration_place) values(%s, %s, %s, %s, %s)""", result)


def build_url(api_key: str, find_by: str, obj: str, region: str, query_data: str, page: str = 1) -> str:
    url = 'https://api.ofdata.ru/v2/search?'
    query = f'key={api_key}&by={find_by}&obj={obj}&region={region}&query={query_data}&page={page}'
    return url + query


@timer
def get_api_data() -> None:
    create_models(cursor)

    api_key = env.get('OFDATA_API_KEY')
    find_by = "okved"
    obj = 'org'
    region = '54'
    okveds = [
        '62.0', '62.01', '62.02', '62.02.1',
        '62.02.2', '62.02.3', '62.02.4', '62.02.9',
        '62.03', '62.03.1', '62.03.11', '62.03.12',
        '62.03.13', '62.03.19', '62.09'
    ]
    for okved in okveds:
        url = build_url(api_key, find_by, obj, region, query_data=okved)
        response = get(url)
        data = response.json()
        write_data(data["data"]["Записи"], okved)

        if data["data"]["СтрВсего"] > 1:
            while data["data"]["СтрТекущ"] != data["data"]["СтрВсего"]:
                url = build_url(api_key, find_by, obj, region, query_data=okved, page=data["data"]["СтрТекущ"] + 1)
                response = get(url)
                data = response.json()
                write_data(data["data"]["Записи"], okved)

    cursor.close()
    connection.close()


if __name__ == '__main__':
    get_api_data()
