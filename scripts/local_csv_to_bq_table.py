import os
import io
import glob
import subprocess
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.api_core.exceptions import NotFound

#------------------------------------------------------------------------------------------------------------------------------

load_dotenv()

#------------------------------------------------------------------------------------------------------------------------------

os.system('gcloud auth application-default login')

#------------------------------------------------------------------------------------------------------------------------------

def find_most_recent_csv(folder_path):
    """
    Finds the path to the most recently modified CSV file in a given folder.

    Args:
        folder_path (str): The path to the folder to search.

    Returns:
        str: The full path to the most recent CSV file, or None if no CSV files are found.
    """
    # Check if the folder exists
    if not os.path.isdir(folder_path):
        print(f"Error: Folder not found at {folder_path}")
        return None

    # Use glob to get a list of all .csv files in the directory
    list_of_csv_files = glob.glob(os.path.join(folder_path, '*.csv'))

    # If the list is empty, there are no CSV files in the folder
    if not list_of_csv_files:
        print("No CSV files found in the folder.")
        return None

    # Use the max() function with the os.path.getmtime as the key
    # This finds the file with the largest modification timestamp
    most_recent_csv_file = max(list_of_csv_files, key=os.path.getmtime)
    
    return most_recent_csv_file
    
#------------------------------------------------------------------------------------------------------------------------------

PROJECT_ID      = os.getenv('ID_BQ_PROJECT')
DATASET_ID      = os.getenv('ID_DATASET')
SOURCE_TABLE_ID = os.getenv('ID_TABLE_SOURCE')
NEW_TABLE_ID    = os.getenv('ID_TABLE_NEW')
CSV_FILE_PATH   = find_most_recent_csv(os.getenv('PATH_LOCAL_DOWNLOADS'))

#------------------------------------------------------------------------------------------------------------------------------

# Initialize a BigQuery client
client = bigquery.Client(project=PROJECT_ID)

#------------------------------------------------------------------------------------------------------------------------------

# ------------------ STEP 1: Get Metadata from Source Table ------------------
# Full table ID for the source table
source_table_ref = client.dataset(DATASET_ID).table(SOURCE_TABLE_ID)
print(f"Retrieving metadata from source table: {source_table_ref.path}...")

try:
    # Get the table object which contains the metadata
    source_table = client.get_table(source_table_ref)
    print("Metadata retrieved successfully.")
except NotFound:
    print(f"Error: Source table '{SOURCE_TABLE_ID}' not found in dataset '{DATASET_ID}'.")

# Extract the necessary metadata
source_schema       = source_table.schema
source_partitioning = source_table.time_partitioning
source_clustering   = source_table.clustering_fields

for idx, field in enumerate(source_schema):
    if field.name == 'ingested_at': source_schema.pop(idx) 

source_schema_dict = {field.name:field.field_type for field in source_schema}
source_schema_dict

#------------------------------------------------------------------------------------------------------------------------------

# new_schema_dict = {
#      'last_modified_at'         : 'TIMESTAMP'
#     ,'transaction_created_at'   : 'TIMESTAMP'
#     ,'internal_id'              : 'STRING'
#     ,'line_id'                  : 'INTEGER'
#     ,'is_mainline'              : 'STRING'
#     ,'transaction_type'         : 'STRING'
#     ,'document_number'          : 'STRING'
#     ,'status'                   : 'STRING'
#     ,'from_location'            : 'STRING'
#     ,'to_location'              : 'STRING'
#     ,'sales_channel'            : 'STRING'
#     ,'department'               : 'STRING'
#     ,'sku'                      : 'STRING'
#     ,'quantity'                 : 'INTEGER'
#     ,'amount'                   : 'NUMERIC'
# }

new_schema_dict = new_schema_dict if 'new_schema_dict' in locals() else source_schema_dict

new_schema = [SchemaField(name=name, field_type=type, mode='NULLABLE') for name, type in new_schema_dict.items()]

#------------------------------------------------------------------------------------------------------------------------------

# ------------------ STEP 2: Create the New Table ------------------
# Full table ID for the new destination table
new_table_ref = client.dataset(DATASET_ID).table(NEW_TABLE_ID)

# Check if the destination table already exists
try:
    client.get_table(new_table_ref)
    print(f"Error: Destination table '{NEW_TABLE_ID}' already exists.")
except NotFound:
    # Table does not exist, so we can proceed with creation
    pass

# Create a new Table object for the destination table
new_table = bigquery.Table(
     new_table_ref
    ,schema= new_schema
    )

# Apply the partitioning and clustering from the source table to the new table
if source_partitioning:
    new_table.time_partitioning = source_partitioning
    print("Applied time partitioning from source table.")

if source_clustering:
    new_table.clustering_fields = source_clustering
    print("Applied clustering from source table.")

try:
    # Create the new table in BigQuery
    client.create_table(new_table)
    print(f"Successfully created new table: {new_table_ref.path}")
except Exception as e:
    print(f"Error creating table: {e}")

#------------------------------------------------------------------------------------------------------------------------------

# ------------------ STEP 3: Load Data from CSV to New Table ------------------
if not os.path.exists(CSV_FILE_PATH):
    print(f"Error: CSV file not found at path: {CSV_FILE_PATH}")

# Configure the load job
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,  # Skip the header row
    autodetect=False,     # We are using a predefined schema
)

# Open the local CSV file
with open(CSV_FILE_PATH, "rb") as source_file:
    print(f"Starting load job from CSV file: {CSV_FILE_PATH}...")
    # Start the load job
    job = client.load_table_from_file(
         source_file
        ,new_table_ref
        ,job_config=job_config
    )

# Wait for the job to complete
try:
    job.result()  # Waits for the job to finish
    print("Load job completed successfully.")
    print(f"Table '{NEW_TABLE_ID}' now has {job.output_rows} rows.")
except Exception as e:
    print(f"Error during load job: {e}")

#------------------------------------------------------------------------------------------------------------------------------

# ------------------ STEP 4: Add and Populate the 'ingested_at' column ------------------
print("Adding and populating 'ingested_at' column with CURRENT_TIMESTAMP()...")

# SQL to add the new column
add_column_sql = f"""
    ALTER TABLE `{PROJECT_ID}.{DATASET_ID}.{NEW_TABLE_ID}`
    ADD COLUMN ingested_at TIMESTAMP;

    ALTER TABLE `{PROJECT_ID}.{DATASET_ID}.{NEW_TABLE_ID}`
    ALTER COLUMN ingested_at SET DEFAULT CURRENT_TIMESTAMP();

    UPDATE `{PROJECT_ID}.{DATASET_ID}.{NEW_TABLE_ID}`
    SET ingested_at = CURRENT_TIMESTAMP()
    WHERE TRUE;
    """

try:
    query_job = client.query(add_column_sql)
    query_job.result()
    print("Successfully added the 'ingested_at' column.")
except Exception as e:
    print(f"Error adding or updating 'ingested_at' column: {e}")

#------------------------------------------------------------------------------------------------------------------------------

# ------------------ STEP 5: Delete Source and Rename New Table ------------------
print("Deleting source table and renaming new table...")

# SQL to rename the new table
rename_sql = f"""
    ALTER TABLE `{PROJECT_ID}.{DATASET_ID}.{NEW_TABLE_ID}`
    RENAME TO `{SOURCE_TABLE_ID}`
    """

try:
    # First, delete the old source table
    client.delete_table(source_table_ref, not_found_ok=True)
    print(f"Successfully deleted old source table: {SOURCE_TABLE_ID}")

    # Then, rename the new table to the old table's name
    query_job = client.query(rename_sql)
    query_job.result()
    print(f"Successfully renamed new table to: {SOURCE_TABLE_ID}")
except Exception as e:
    print(f"Error during table rename process: {e}")