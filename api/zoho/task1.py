import os
import boto3
import psycopg2
import pandas as pd
import warnings
from datetime import datetime
from io import StringIO

warnings.filterwarnings("ignore", category=UserWarning, message="pandas only supports SQLAlchemy")

s3_client = boto3.client('s3')

BUCKET_NAME = os.getenv("BUCKET_RAW_NAME")
DB_CONFIG = {
    "host": os.getenv("DB_ZOHO_INSTANCE_IP"),
    "port": os.getenv("DB_ZOHO_INSTANCE_PORT"),
    "user": os.getenv("DB_ZOHO_USER"),
    "password": os.getenv("DB_ZOHO_PASSWORD"),
    "database": os.getenv("DB_ZOHO_DATABASE"),
}
TODAY = datetime.now().strftime('%Y-%m-%d')

def lambda_handler(event, context):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    list_tables_query = """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('information_schema', 'pg_catalog')
    ORDER BY table_schema, table_name;
    """
    cur.execute(list_tables_query)

    tables = cur.fetchall()

    for schema, table in tables:
        s3_path = f"autoprovision/zoho-crm/{TODAY}/{schema}__{table}.csv"
        df = pd.read_sql_query(f"SELECT * FROM {schema}.{table};", conn)

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, sep=';')

        s3_client.put_object(
        	Bucket=BUCKET_NAME,
        	Key=s3_path,
        	Body=csv_buffer.getvalue()
        )

def handler():
    return lambda_handler({}, {})
