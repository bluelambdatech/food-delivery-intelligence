import snowflake.connector as sc
from snowflake.connector.pandas_tools import pd_writer
from sqlalchemy import create_engine as cen
import pandas as pd
from decouple import config
from dotenv import load_dotenv
import os

from dotenv import dotenv_values

# config = dotenv_values(".env")
# print(config)

load_dotenv()

user = os.getenv("user")
password = os.getenv("password")

ctx = sc.connect(
    user='USER',
    password='password',
    account="ACCOUNT",
    schema='SCHEMA',
    database='DATABASE',
    role='ROLE',
    warehouse = "WAREHOUSE"
    )
# cur = ctx.cursor()
# cur.execute("SELECT * FROM STORE")
# try:
#     cur.execute("SELECT current_version()")
#     one_row = cur.fetchone()
#     print(one_row[0])
# finally:
#     cur.close()
# ctx.close()


# Create SparkSession from builder
#import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.master("local[1]") \
#                     .appName('SparkByExamples.com') \
#                     .getOrCreate()

#using SQLAlchemy to connect to snowflake
"""using the to_sql method is best practice compared to write_pandas because you don't worry if 
the table exists or not """

# account_identifier = 'account_identifier'
# user = 'user'
# password = 'password'
# database_name = 'TESTDB'
# schema_name = 'TESTSCHEMA'



# conn_string = f"snowflake://{user}:{password}@{account}/{database}/{schema}"
# engine = cen(conn_string)


"""create a wh"""

# ctx.cursor().execute("CREATE WAREHOUSE IF NOT EXISTS tiny_warehouse_mg")
#
# """create a db,"""
# ctx.cursor().execute("CREATE DATABASE IF NOT EXISTS testdb")
#
# """create a schema"""
# ctx.cursor().execute("CREATE SCHEMA IF NOT EXISTS testschema")
#
# """create a table"""
# ctx.cursor().execute(
#     "CREATE OR REPLACE TABLE "
#     "test_table(col1 integer, col2 string)")
#load_dotenv()

#  user = os.getenv("user")
#  password = os.getenv("password")
#  account = os.getenv('account')
#  authenticator = os.getenv('authenticator')

# table_name = 'test_table'
# df = pd.read_csv(r"C:\Users\munac\My_Projects\chunk_1.csv")
# if_exists = 'replace'
#
# with engine.connect() as con:
#     df.to_sql(name=table_name.lower(), con=con, if_exists=if_exists, index=False, method=pd_writer)

# class SnowConn:
#      pass
# #
# def __init__(self):
# #     pass
#     def Snow_conn(self, cur):
#         self.conn_string = f"snowflake://{user}:{password}@{account_identifier}/{database_name}/{schema_name}"
#     engine = cen(self.conn_string)
#     return(engine)
#
# def Read_snowflake(self):
#     self.cur = self.conn.cursor()
#     self.cur.execute('SELECT * FROM STORE')
#     df = pd.DataFrame(self.cur.fetchall(), columns=self.cur.description)
#     df.columns = [col[0] for col in df.columns]
#     df.head()
#     df.columns = [col[0] for col in df.columns]
# def Write_snowflake():
#     passtable_name = 'test_table'
# df = pd.read_csv(r"C:\Users\munac\My_Projects\train.csv")
# if_exists = 'replace'
# with engine.connect() as con:
#     df.to_sql(name=table_name.lower(), con=con, if_exists=if_exists, method=pd_write)