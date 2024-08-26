import clickhouse_connect
import os
from dotenv import load_dotenv

load_dotenv()

# Configuração do cliente ClickHouse
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST')
CLICKHOUSE_PORT = os.getenv('CLICKHOUSE_PORT')

def get_client():
    return clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)

def execute_sql_script(script_path):
    client = get_client()
    with open(script_path, 'r') as file:
        sql_script = file.read()
    client.command(sql_script)
    return client

def insert_dataframe(client, table_name, df):
    client.insert_df(table_name, df)
