from sqlalchemy import *
from dotenv.main import load_dotenv
import os

load_dotenv()

def get_db_connection():
    username = os.environ['DB_USERNAME']
    password = os.environ['DB_PASSWORD']
    host = os.environ['DB_HOST']
    port = os.environ['DB_PORT']
    db_name = os.environ['DB_NAME']

    conn_str = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(conn_str)
    conn = engine.connect()
    return conn

def get_db_engine():
    username = os.environ['DB_USERNAME']
    password = os.environ['DB_PASSWORD']
    host = os.environ['DB_HOST']
    port = os.environ['DB_PORT']
    db_name = os.environ['DB_NAME']

    conn_str = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(conn_str)
    return engine
