from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from dotenv import load_dotenv 
from urllib import parse
import os

load_dotenv()


engine = create_engine(URL(
    account=os.getenv('ACCOUNT_IDENTIFIER'),
    user =os.getenv('USER'),
    password=parse.quote(os.getenv('PASSWORD')),
    database = 'SNOWFLAKE_SAMPLE_DATA',
    schema = 'TPCDS_SF10TCL',
    warehouse = 'STREAMLIT',
    role='ACCOUNTADMIN',
    timezone = 'America/Los_Angeles',
))
connection = engine.connect()



connection.close()
engine.dispose()