import os
import subprocess
import zipfile
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector

# -----------------------------------
# Load environment variables
# -----------------------------------
load_dotenv()

DATA_PATH = "data"
DATASET_NAME = "zillow/zecon"
ZIP_FILE = os.path.join(DATA_PATH, "zecon.zip")
CSV_FILE = os.path.join(DATA_PATH, "State_time_series.csv")
FINAL_FILE = os.path.join(DATA_PATH, "final_data.csv")

# -----------------------------------
# 1. Download dataset
# -----------------------------------
print("Downloading dataset...")

subprocess.run([
    "kaggle",
    "datasets",
    "download",
    "-d",
    DATASET_NAME,
    "-p",
    DATA_PATH
], check=True)

# -----------------------------------
# 2. Unzip dataset
# -----------------------------------
with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
    zip_ref.extractall(DATA_PATH)

print("Unzipped successfully.")

# -----------------------------------
# 3. Load CSV
# -----------------------------------
df = pd.read_csv(CSV_FILE)

print("Rows loaded:", len(df))
print("Columns found:", len(df.columns))

# Save clean CSV
df.to_csv(FINAL_FILE, index=False)

# -----------------------------------
# 4. Connect to Snowflake
# -----------------------------------
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    role=os.getenv("SNOWFLAKE_ROLE")
)

cursor = conn.cursor()

# -----------------------------------
# 5. Dynamically Create Table
# -----------------------------------
columns_sql = ",\n".join([f'"{col}" STRING' for col in df.columns])

create_table_sql = f"""
CREATE OR REPLACE TABLE KAGGLE_DATA (
{columns_sql}
)
"""

cursor.execute(create_table_sql)

print("Table created successfully.")

# -----------------------------------
# 6. Create Stage
# -----------------------------------
cursor.execute("CREATE OR REPLACE STAGE KAGGLE_STAGE")

# -----------------------------------
# 7. Upload File to Stage
# -----------------------------------
cursor.execute(f"""
PUT file://{os.path.abspath(FINAL_FILE)} @KAGGLE_STAGE AUTO_COMPRESS=TRUE
""")

print("File uploaded to stage.")

# -----------------------------------
# 8. Bulk Load into Table
# -----------------------------------
cursor.execute("""
COPY INTO KAGGLE_DATA
FROM @KAGGLE_STAGE
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
""")

print("Data loaded into Snowflake successfully!")

cursor.close()
conn.close()
