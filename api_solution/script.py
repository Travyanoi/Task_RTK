from requests import get
from dotenv import load_dotenv
from os import environ as env

from utils.db_connect import cursor, connection
from utils.utils import timer
from utils.models import create_models

load_dotenv()


def write_data(data: dict, okved: str) -> None:
    """Func to write data from ofdata API to database
    :param data: data from ofdata API to write
    :param okved: okved to write in database"""
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
    """Func bulds url to get data from ofdata.ru API\n
    :param api_key: Api key
    :param find_by: The filter by which the api searches for data from query_data
    :param obj: object to be searched: org or ent
    :param region: region to be searched 2 nums
    :param query_data: query for searching
    :param page: num of page if more than 1"""
    if obj != "org" or obj != "ent":
        raise ValueError(f"obj must be org or ent, got {obj}")
    if len(region) != 2:
        raise ValueError(f"region must be 2 nums, got {region}")

    url = 'https://api.ofdata.ru/v2/search?'
    query = f'key={api_key}&by={find_by}&obj={obj}&region={region}&query={query_data}&page={page}'
    return url + query


@timer
def get_api_data() -> None:
    """Func get data from ofdata.ru API"""
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
