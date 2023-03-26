import snowflake.connector as sc
from snowflake.connector.pandas_tools import pd_writer
from sqlalchemy import create_engine as cen
import pandas as pd
from decouple import config
from dotenv import load_dotenv
import os

from dotenv import dotenv_values
import yaml
config = dotenv_values(".env")


load_dotenv()

user = os.getenv("user")
password = os.getenv("password")

ctx = sc.connect(
    user='BRANNNNY',
    password='Chinyere1',
    account='ZDHHCNK-CHB49113',
    database="SNOWFLAKE_SAMPLE_DATA",
    schema="TPCDS_SF100TCL",
    warehouse="COMPUTE_WH"
    )


def parse_config(path):
    with open(path, 'r') as file:
        prime_service = yaml.safe_load(file)
        prime_service["sf_cred"]["user"] = os.getenv("user")
        prime_service["sf_cred"]["password"] = os.getenv("password")
    return prime_service['sf_cred']


class SnowConn:
    def __int__(self, path):
        config = parse_config(path)
        self.ctx = sc.connect(**config)

    def read_from_sf(self):
        pass

    def write_to_sf(self):
        pass

    def create_wh(self):
        pass

    def create_db(self):
        pass

    def create_schema(self, schemaName, warehouse, database):

        self.ctx.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {schemaName}")

    def create_table(self):
        pass




with open('config.yml', 'r') as file:
    prime_service = yaml.safe_load(file)

print(prime_service['sf_cred'])