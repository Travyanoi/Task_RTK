from os import environ as env

from dotenv import load_dotenv
import psycopg2

load_dotenv()

connection = psycopg2.connect(
    database=env.get('DB_NAME'),
    user=env.get('DB_USER'),
    password=env.get('DB_PASS'),
    host="127.0.0.1",
    port=env.get('DB_PORT')
)

connection.autocommit = True
cursor = connection.cursor()
