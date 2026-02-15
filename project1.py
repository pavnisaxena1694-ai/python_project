import os
import subprocess
import zipfile
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()
data_path ="data"
dataset_name ='ankitbansal06/retail-orders'
datafile="orders.csv"
print("Downloading  dataset...")


subprocess.run([
    "kaggle",
    "datasets",
    "download",
    "-d",
    dataset_name,
    "-p",
    data_path,
    "--unzip"
], check=True)

df=pd.read_csv("data/orders.csv",na_values=['Not Available','unknown'])
df['Ship Mode'].unique()

#df.rename(columns={'Order _Id':'order_id'})
df.columns = df.columns.str.lower()
df.columns= df.columns.str.replace(' ','_')

df['discount']= df['list_price']* df['discount_percent']*.01
df['sales_price'] = df['list_price'] - df['discount']
df['profit'] =df['sales_price']-df['cost_price']

df['order_date'] = pd.to_datetime(df['order_date'],format="%Y-%m-%d")
#print(df['order_date'])

df.drop(columns=['list_price','cost_price','discount_percent'],inplace=True)
#print (df)

conn = snowflake.connector.connect (
     user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    role=os.getenv("SNOWFLAKE_ROLE")
)
cursor = conn.cursor()

column_defination=[]
for column_name,dtype in df.dtypes.items():
    if "int" in str(dtype):
        snowflake_type="INTEGER"
    elif "float" in str(dtype):
        snowflake_type="FLOAT"
    elif "datetime" in str(dtype):
        snowflake_type="TIMESTAMP"
    else:
        snowflake_type="STRING"
    column_defination.append(f'"{column_name}"{snowflake_type}')

columns_sql =",\n".join(column_defination)
#to save place
#CREATE TABLE IF NOT EXISTS DF_ORDERS (...)
create_table_sql= f"""Create or replace table DF_ORDERS(
{columns_sql}
)
"""
cursor.execute(create_table_sql)
print("Table created")

write_pandas(conn,df,'DF_ORDERS')


print("Table Loaded")
