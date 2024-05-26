from os import path, mkdir, remove
from time import time
from json import loads
from requests import get

from utils.db_connect import cursor


def timer(func):
    """Just a decorator timer"""

    def wrapper(*args, **kwargs) -> None:
        start = time()
        func(*args, **kwargs)
        print(f"Elapsed time = {time() - start} sec")

    return wrapper


def check_zip_exists(file_path: str) -> None:
    """Check zip file exists
    :param file_path: Zip file path"""

    if not path.exists(file_path):
        print(f"File does not exist, download... It can take a while\n")
        response = get('https://ofdata.ru/open-data/download/egrul.json.zip')
        with open(file_path, 'wb') as f:
            f.write(response.content)
        print(f"File has been downloaded\n")


def read_json_file(data: bytes, encoding: str = 'utf-8') -> dict:
    """Read json file from bytes
    :param data: Json file data
    :param encoding: Json file encoding"""

    return loads(data.decode(encoding))


def search_okved(data: dict, code_okved: str) -> str:
    """Search okved in data
    :param data: Json file data
    :param code_okved: Okved code to search"""

    if data["СвОКВЭДОсн"]["КодОКВЭД"].startswith(code_okved):
        return data["СвОКВЭДОсн"]["КодОКВЭД"]

    try:
        if isinstance(data["СвОКВЭДДоп"], list):
            for sub_data in data["СвОКВЭДДоп"]:
                if sub_data["КодОКВЭД"].startswith(code_okved):
                    return sub_data["КодОКВЭД"]
        else:
            if data["СвОКВЭДДоп"]["КодОКВЭД"].startswith(code_okved):
                return data["СвОКВЭДДоп"]["КодОКВЭД"]

    except KeyError:
        return 'None'

    return 'None'


def build_registration_place(data: dict) -> str:
    """This function builds full registration place based on data.\n
        :param data: Dict with information about place of registration."""

    region_info = data["Регион"]
    city = data["Город"]
    street = data["Улица"]

    address = f"{data['Индекс']}, {region_info['НаимРегион']} {region_info['ТипРегион']}, " \
              f"{city['ТипГород']} {city['НаимГород']}, {street['ТипУлица']} {street['НаимУлица']}, " \
              f"{data['Дом']}, {data['Корпус']}"

    return address


def search_and_write(data: dict) -> None:
    """Search needed data and write it to database
    :param data: Dict with information organization."""
    for org in data:
        try:
            okved = search_okved(org["data"]["СвОКВЭД"], "62")
            region: str = org["data"]["СвАдресЮЛ"]["АдресРФ"]["КодРегион"]
            if okved != 'None' and region == "54":
                inn = org["inn"]
                kpp = org["kpp"]
                full_name = org["full_name"]
                registration_place = build_registration_place(org["data"]["СвАдресЮЛ"]["АдресРФ"])

                cursor.execute("""insert into okved_62_org_info(
                                    company_name,
                                    okved,
                                    inn,
                                    kpp,
                                    registration_place) values(%s, %s, %s, %s, %s)""",
                               (full_name, okved, inn, kpp, registration_place))

        except KeyError:
            continue
