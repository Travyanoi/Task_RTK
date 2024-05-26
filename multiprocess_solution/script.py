import multiprocessing
import zipfile

from utils.models import create_models
from utils.utils import check_zip_exists, timer, read_json_file, search_and_write
from utils.db_connect import cursor, connection


def process_chunk(zip_file_path: str, chunk: list[str]) -> None:
    """Reads a zip file, converts to json, search and writes it to a file.
    :param zip_file_path: Zip file path
    :param chunk: List of filenames in zip file"""
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        for current_name in chunk:
            data = read_json_file(zip_ref.read(current_name))
            search_and_write(data)


def separate_on_chunks(list_of_names: list[str], zip_file_path: str) -> None:
    """Func for separating on chunks list of names and give it to processes
    :param list_of_names: List of names of files
    :param zip_file_path: Zip file path"""
    num_workers = multiprocessing.cpu_count()
    chunk_size = len(list_of_names) // num_workers

    chunks = [list_of_names[i:i + chunk_size] for i in range(0, len(list_of_names), chunk_size)]

    processes = []
    for chunk in chunks:
        process = multiprocessing.Process(target=process_chunk, args=(zip_file_path, chunk))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()


def get_names_zip_file(zip_file_path: str) -> list[str]:
    """Func get the list of names from zip file and return list of names.
    :param zip_file_path: Zip file path"""
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        return zip_ref.namelist()


@timer
def main() -> None:
    zip_file_path = 'D:/PyCharmProjects/Task_RTK/egrul.json.zip'

    create_models(cursor)
    check_zip_exists(zip_file_path)

    list_of_names = get_names_zip_file(zip_file_path)
    separate_on_chunks(list_of_names, zip_file_path)

    cursor.close()
    connection.close()


if __name__ == '__main__':
    main()
