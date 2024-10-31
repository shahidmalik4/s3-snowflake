import boto3
import pandas as pd
from io import StringIO
from dotenv import load_dotenv
import snowflake.connector
import os
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

s3_bucket_name = 's3-snowflake-etl-pipeline'
s3_file_key = 'data/AIRBNB_NYC.csv'

# AWS Credentials
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

# Initialize S3 Client and Extract CSV from S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

def load_data_from_s3(bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    data = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(data))
    return df

df = load_data_from_s3(s3_bucket_name, s3_file_key)

# Transform the Data with Pandas (Modify as Needed)
df['new_column'] = df['host_id'] * 2

# Snowflake connection parameters
sf_user = os.getenv('SNOWFLAKE_USER')
sf_password = os.getenv('SNOWFLAKE_PASSWORD')
sf_account = os.getenv('SNOWFLAKE_ACCOUNT')
sf_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
sf_database = os.getenv('SNOWFLAKE_DATABASE')
sf_schema = os.getenv('SNOWFLAKE_SCHEMA')
sf_table = os.getenv('SNOWFLAKE_TABLE')

# Create a connection to Snowflake
cnx = snowflake.connector.connect(
    user=sf_user,
    password=sf_password,
    account=sf_account,
    warehouse=sf_warehouse,
    database=sf_database,
    schema=sf_schema,
)

# Load Data into Snowflake using write_pandas
try:
    success, nchunks, nrows, _ = write_pandas(cnx, df, sf_table, auto_create_table=True)
    
    if success:
        print(f"Data successfully loaded into Snowflake table '{sf_table}' with {nrows} rows.")
    else:
        print("Failed to load data into Snowflake.")
except Exception as e:
    print("Error loading data into Snowflake:", e)
finally:
    cnx.close()
