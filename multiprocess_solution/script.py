import multiprocessing
import zipfile

from utils.models import create_models
from utils.utils import check_zip_exists, timer, read_json_file, search_and_write
from utils.db_connect import cursor, connection


def process_chunk(zip_file_path: str, chunk: list) -> None:
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        for current_name in chunk:
            data = read_json_file(zip_ref.read(current_name))
            search_and_write(data)


def unzip_file(zip_file_path: str) -> None:
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        list_of_names = zip_ref.namelist()

    num_workers = multiprocessing.cpu_count()
    chunk_size = len(list_of_names) // num_workers

    chunks = [list_of_names[i:i+chunk_size] for i in range(0, len(list_of_names), chunk_size)]

    processes = []
    for chunk in chunks:
        process = multiprocessing.Process(target=process_chunk, args=(zip_file_path, chunk))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()


@timer
def main() -> None:
    zip_file_path = 'D:/PyCharmProjects/Task_RTK/egrul.json.zip'

    create_models(cursor)
    check_zip_exists(zip_file_path)

    unzip_file(zip_file_path)

    cursor.close()
    connection.close()


if __name__ == '__main__':
    main()
