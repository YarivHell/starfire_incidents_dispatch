import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.bigquery import SchemaField, Table, LoadJobConfig
import os
import json
import pyodbc
import sqlalchemy
import numpy as np
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


def write_to_table(df_to_tbl):
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "AIzaSyC4qsSBp83L3IRohxjdoXXhFiSuM3RtWcs"
    with open('my_gcp_key.json', 'r') as f:
        config = json.load(f)

    # Access the values in the dictionary
    cloud_key = config['cloud_key']
    api_key = config['api_key']

    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = api_key
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cloud_key


    client = bigquery.Client()

    # Create a new dataset
    dataset_id = 'dispatch'
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = 'US'
    dataset = client.create_dataset(dataset)  # API request
    print(f'Created dataset {dataset.dataset_id} in location {dataset.location}')

    # # Define the schema table
    # schema = []
    # for i in df_to_tbl.columnns:
    #     schema += SchemaField(df_to_tbl.columnns[i], 'STRING')

    table_id = 'dispatch.incident'
    table = Table(table_id, schema='Dispatch')

    temp_df = df_to_tbl.column.endswith('_id')

    client.create_table(table)  # API request
    temp_df.to_gbq(destination_table=table_id, project_id=client.project, if_exists='append')
    # df_to_tbl.to_gbq(destination_table=table_id, project_id=client.project, if_exists='append')


# def write_to_sql(df_to_tbl):
#     dsn = 'Yariv'
#     database = 'NYC_DB'
#     table_name = 'Dispatch.Incidents'
#     username = 'DESKTOP-JJP3BVB\Yariv'
#     server = 'DESKTOP-JJP3BVB'
#     # conn = pyodbc.connect(f'DSN={dsn};Database={database};UID={username}')
#     conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};' +
#                           f'SERVER={server};' +
#                           f'DATABASE={database};' +
#                           'Trusted_Connection=yes;' +
#                           f'UID={username};')
#     df_to_tbl.to_sql(table_name, conn, if_exists='append', index=False)
def write_to_sql(df_to_tbl):
    server = 'DESKTOP-JJP3BVB'
    database = 'NYC_DB'
    username = 'DESKTOP-JJP3BVB\\Yariv'
    table_name = 'Dispatch.Incidents'
    driver = 'SQL Server'
    trusted_connection = 'yes'
    connection_string = f'mssql+pyodbc://{username}@{server}/{database}?driver={driver}&Trusted_Connection={trusted_connection}'


    # Create the SQLalchemy engine object
    engine = create_engine(connection_string)
    metadata = MetaData()
    incidents = sqlalchemy.Table(table_name, metadata,
                  Column('highest_alarm_level', Integer, primary_key=False))
    conn = engine.connect()
    # insert_query = incidents.insert().values(**temp_df)
    conn.execute(incidents.insert(), df_to_tbl.to_dict())
    conn.close()
    # df_to_tbl.to_sql(table_name, con=conn, if_exists='append', index=False, method='multi')
    # temp_df.to_sql(table_name, con=conn, if_exists='append', index=False, method='multi')


def create_table(df_org):
    server = 'DESKTOP-JJP3BVB'
    database = 'NYC_DB'
    username = 'DESKTOP-JJP3BVB\\Yariv'
    table_name = 'Dispatch.Incidents'
    driver = 'SQL Server'
    trusted_connection = 'yes'
    connection_string = f'mssql+pyodbc://{username}@{server}/{database}?driver={driver}&Trusted_Connection={trusted_connection}'
    engine = create_engine(connection_string)

    # create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    # declare the base
    Base = declarative_base()
    # define the table class
    class incidents(Base):
        __tablename__ = 'Test'
        starfire_incident_id = Column(Integer, primary_key=True)

    for col_name, col_dtype in zip(df_org.columns, df_org.dtypes):
        if col_dtype == 'object':
            setattr(incidents, col_name, Column(String))
        elif col_name.endswith('_id'):
            continue
        elif col_dtype == 'int32':
            setattr(incidents, col_name, Column(Integer))
        elif col_dtype == 'datetime64[ns]':
            setattr(incidents, col_name, Column(DateTime))
    # create the table
    Base.metadata.create_all(engine)


def generate_create_table_sql(df, table_name):
    # Get column names and types
    columns = []
    for col_name, dtype in df.dtypes.items():
        sql_type = None
        if dtype == 'int32' and col_name.endswith('_id'):
            sql_type = 'INT IDENTITY(1,1) PRIMARY KEY'
        if dtype == 'int32':
            sql_type = 'INT'
        elif dtype == 'object':
            sql_type = 'VARCHAR(MAX)'
        elif dtype == 'datetime64[ns]':
            sql_type = 'DATETIME'
        if sql_type:
            columns.append(f'[{col_name}] {sql_type}')

    # Add id column
    # columns.insert(0, '[id] INT IDENTITY(1,1) PRIMARY KEY')

    # Build SQL statement
    sql = f'CREATE TABLE [{table_name}] ({", ".join(columns)})'

    return sql


def get_api():
    response = requests.get('https://data.cityofnewyork.us/resource/8m42-w767.json')
    if response.status_code == 200:
        json_data = response.json()
        df = pd.DataFrame(json_data)
        print(df.head())
    else:
        print('API request failed with status code', response.status_code)
    return df


def fix_types(df):
    type_mapping = {
        'int': lambda x: int(x.replace(',', '')),
        'datetime64': lambda x: pd.to_datetime(x, format='%Y-%m-%dT%H:%M:%S.%f', errors='coerce'),
        'str': lambda x: str(x)
    }
    # loop over columns and convert to appropriate type based on values
    for col in df.columns:
        col_values = df[col].unique()
        for t, func in type_mapping.items():
            try:
                if all(isinstance(func(val), eval(t)) for val in col_values):
                    df[col] = df[col].apply(func).astype(eval(t))
                    if col.endswith('_datetime'):
                        df[col] = pd.to_datetime(df[col])
                    break
            except:
                continue
    df = df.loc[:, ~df.columns.duplicated()]
    return df


if __name__ == '__main__':
    df_org = get_api()
    df_to_tbl = fix_types(df_org)
    #
    # table_name = 'Test'
    # create_table_sql = generate_create_table_sql(df_to_tbl, table_name)
    #
    # server = 'DESKTOP-JJP3BVB'
    # database = 'NYC_DB'
    # username = 'DESKTOP-JJP3BVB\\Yariv'
    # driver = 'SQL Server'
    # trusted_connection = 'yes'
    # connection_string = f'mssql+pyodbc://{username}@{server}/{database}?driver={driver}&Trusted_Connection={trusted_connection}'
    # engine = create_engine(connection_string)
    #
    # # Execute SQL statement
    # with engine.connect() as conn:
    #     conn.execute(create_table_sql)

    create_table(df_to_tbl)
    # write_to_table(df_to_tbl)
    # write_to_sql(df_to_tbl)

