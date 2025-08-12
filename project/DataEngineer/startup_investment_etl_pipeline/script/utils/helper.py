from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

from pyspark.sql import SparkSession

load_dotenv(".env")

DB_HOST_SOURCE = os.getenv("DB_HOST_SOURCE")
DB_USER_SOURCE = os.getenv("DB_USER_SOURCE")
DB_PASS_SOURCE = os.getenv("DB_PASS_SOURCE")
DB_PORT_SOURCE = os.getenv("DB_PORT_SOURCE")

DB_HOST_TARGET = os.getenv("DB_HOST_TARGET")
DB_USER_TARGET = os.getenv("DB_USER_TARGET")
DB_PASS_TARGET = os.getenv("DB_PASS_TARGET")
DB_PORT_TARGET = os.getenv("DB_PORT_TARGET")

DB_NAME_SOURCE = os.getenv("DB_NAME_SOURCE")
DB_NAME_STG = os.getenv("DB_NAME_STG")
DB_NAME_LOG = os.getenv("DB_NAME_LOG")
DB_NAME_WH = os.getenv("DB_NAME_WH")

API_PATH = os.getenv("API_PATH")

# Create URL link for each database connection
def source_engine():
    DB_URL = f"jdbc:postgresql://{DB_HOST_SOURCE}:{DB_PORT_SOURCE}/{DB_NAME_SOURCE}"
    return DB_URL, DB_USER_SOURCE, DB_PASS_SOURCE, DB_NAME_SOURCE

def stg_engine():
    DB_URL = f"jdbc:postgresql://{DB_HOST_TARGET}:{DB_PORT_TARGET}/{DB_NAME_STG}"
    return DB_URL, DB_USER_TARGET, DB_PASS_TARGET, DB_NAME_STG

def log_engine():
    DB_URL = f"jdbc:postgresql://{DB_HOST_TARGET}:{DB_PORT_TARGET}/{DB_NAME_LOG}"
    return DB_URL, DB_USER_TARGET, DB_PASS_TARGET, DB_NAME_LOG

def wh_engine():
    DB_URL = f"jdbc:postgresql://{DB_HOST_TARGET}:{DB_PORT_TARGET}/{DB_NAME_WH}"
    return DB_URL, DB_USER_TARGET, DB_PASS_TARGET, DB_NAME_WH

def wh_engine_sqlalchemy():
    return create_engine(f"postgresql://{DB_USER_TARGET}:{DB_PASS_TARGET}@{DB_HOST_TARGET}:{DB_PORT_TARGET}/{DB_NAME_WH}")

def load_log(spark: SparkSession, log_msg):
    DB_URL, DB_USER, DB_PASS, DB_NAME = log_engine()
    table_name = "etl_log"

    # set config
    connection_properties = {
        "user": DB_USER,
        "password": DB_PASS,
        "driver": "org.postgresql.Driver" # set driver postgres
    }

    log_msg.write.jdbc(url = DB_URL,
                      table = table_name,
                      mode = "append",
                      properties = connection_properties)