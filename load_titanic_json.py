import os
import subprocess
from dotenv import load_dotenv
import snowflake.connector

# --------------------------------
# Load env variables
# --------------------------------
load_dotenv()

DATA_PATH = "data"
DATASET = "engrbasit62/titanic-json-format"  # chosen dataset
JSON_FILE = "titanic.json"  # expected JSON file from dataset

# --------------------------------
# 1. Download dataset from Kaggle
# --------------------------------
print("Downloading Titanic JSON dataset...")

subprocess.run([
    "kaggle",
    "datasets",
    "download",
    "-d",
    DATASET,
    "-p",
    DATA_PATH,
    "--unzip"
], check=True)

print("Download complete.")

# --------------------------------
# 2. Connect to Snowflake
# --------------------------------
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

# --------------------------------
# 3. Create target table
# --------------------------------
cursor.execute("""
CREATE OR REPLACE TABLE RAW_JSON_EVENTS (
    event_date DATE,
    source STRING,
    json_data VARIANT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
""")

# --------------------------------
# 4. Create a Snowflake stage
# --------------------------------
cursor.execute("CREATE OR REPLACE STAGE json_stage")

# --------------------------------
# 5. Upload the JSON file
# --------------------------------
json_path = os.path.join(DATA_PATH, JSON_FILE)

cursor.execute(f"""
PUT file://{os.path.abspath(json_path)} @json_stage AUTO_COMPRESS=TRUE
""")

print("JSON file uploaded to stage.")

# --------------------------------
# 6. Load JSON into Snowflake
# --------------------------------
cursor.execute("""
COPY INTO RAW_JSON_EVENTS (event_date, source, json_data)
FROM (
    SELECT
        CURRENT_DATE() as event_date,
        'titanic-json-format' as source,
        $1
    FROM @json_stage
)
FILE_FORMAT = (TYPE = JSON)
ON_ERROR = 'CONTINUE'
""")

print("Data loaded into Snowflake successfully!")

cursor.close()
conn.close()
