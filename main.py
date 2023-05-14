import requests
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, inspect, DateTime, MetaData,text


def write_to_sql(df_to_tbl):
    server = 'DESKTOP-JJP3BVB'
    database = 'NYC_DB'
    username = 'DESKTOP-JJP3BVB\\Yariv'
    table_name = 'Dispatch_Incidents'
    driver = 'SQL Server'
    trusted_connection = 'yes'
    # index_name = 'idx_partition'
    # index_fields = ['incident_datetime']
    # index_partition_name = 'p_partition'
    # index_partition_type = 'RANGE_PARTITION'
    # index_partition_expression = "DATE_TRUNC('%Y-%m-%d', incident_datetime)"
    # index_partition_start = '2021-01-01'
    # index_partition_end = '2024-08-01'
    # index_partition_interval = '1 MONTH'

    # index_cols = [col for col in df_to_tbl.columns if col.endswith('_id')]
    # partition_col = next((col for col in df_to_tbl.columns if col.endswith('_datetime')), None)
#
#     if index_cols and partition_col:
#         partition_format = '%Y-%m-%d'
#         partition_value = pd.to_datetime(df_to_tbl[partition_col]).dt.strftime(partition_format)
#         schema = text(f"""
#     fields:
#         {','.join([col + ': ' + str(df_to_tbl[col].dtypes) for col in df_to_tbl.columns])}
#     primaryKey:
#         {',' .join([col for col in df_to_tbl.columns if col.endswith('_id')])}
#     indexes:
#         - name: idx_partition
#           fields: {partition_col}
#           partition:
#             name: p_partition
#             type: RANGE
#             expression: DATE_TRUNC('{partition_format}', {partition_col})
#             start: {partition_value.min()}
#             end: {partition_value.max()}
#             interval: 1 MONTH
# """)
#     else:
#         schema = None

    # schema_str = str(schema)

    connection_string = f'mssql+pyodbc://{username}@{server}/{database}?driver={driver}&Trusted_Connection={trusted_connection}'
    engine = create_engine(connection_string, use_setinputsizes=False)
    conn = engine.connect()


    memory_usage = df_to_tbl.memory_usage(index=True).sum()
    print(f"The dataframe uses {memory_usage / 1024:.2f} KB of memory.")
    num_rows, num_cols = df_to_tbl.shape
    print("Number of rows:", num_rows)
    print("Number of columns:", num_cols)

    df_to_tbl.to_sql(table_name, con=conn, if_exists='append', index=False, method='multi', chunksize=10)


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
    write_to_sql(df_to_tbl)