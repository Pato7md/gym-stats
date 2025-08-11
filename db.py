import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
db_name = os.getenv('DB_NAME_GYM')

connection_string = f'postgresql://{user}:{password}@{host}:5432/{db_name}'
ENGINE = create_engine(connection_string, connect_args={"options": "-c client_encoding=utf8"})
