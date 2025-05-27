from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Assuming etl scripts are in PYTHONPATH or accessible
# If etl_project is the root of the Airflow project, and Dockerfile copies it to /app
# Python's sys.path might need adjustment or proper packaging
# For simplicity, let's assume they are directly importable or PYTHONPATH is set in Docker.
# One common way is to add the project directory to sys.path
import sys
import os
# Add the parent directory of 'etl' to sys.path to allow imports like 'from etl.extract import extract'
# This assumes the DAG file is in etl_project/dags and scripts are in etl_project/etl
APP_DIR = "/app" # This should match the WORKDIR in your Dockerfile
sys.path.append(APP_DIR)

from etl.extract import extract
from etl.infer_types import infer_types_lama
from etl.validate import auto_cast_and_validate
from etl.transform import transform
from etl.load import load_to_postgres # load_to_postgres now expects type_map

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_pipeline():
    # Path to the sample data file within the Docker container
    # This path will be relative to the WORKDIR /app if data is copied to /app/data
    sample_file_path = os.path.join(APP_DIR, "data", "sample.csv") 
    
    print(f"Starting ETL pipeline for file: {sample_file_path}")
    
    df = extract(sample_file_path)
    print(f"Extracted DataFrame with {len(df)} rows and columns: {df.columns.tolist()}")
    if df.empty:
        print("Extracted DataFrame is empty. Skipping further processing.")
        return

    # Pass a copy to infer_types_lama as it might add/remove columns
    df_for_inference = df.copy()
    type_map = infer_types_lama(df_for_inference) 
    print(f"Inferred type map: {type_map}")
    
    # Pass a copy for validation to keep original df intact if needed
    df_for_validation = df.copy()
    # The type_map from LAMA should guide validation and casting
    df_validated, cast_errors, validation_errors = auto_cast_and_validate(df_for_validation, type_map)
    
    print(f"Casting errors encountered: {cast_errors}")
    # Example: print first 5 validation errors if many
    if validation_errors:
        print(f"Validation errors encountered (first 5 shown): {validation_errors[:5]}")
        # Potentially, you could decide to stop the pipeline or filter rows based on validation_errors
        # For now, we proceed with df_validated which has NaNs for uncastable/invalid values.
    else:
        print("No validation errors.")

    df_transformed = transform(df_validated)
    print(f"Transformation step completed. DataFrame shape: {df_transformed.shape}")
    
    # Key change: Pass the `type_map` to `load_to_postgres`
    # The base table name is 'clean_data'. The load function will handle versioning.
    print(f"Loading data to PostgreSQL. Base table name: 'clean_data'.")
    load_to_postgres(df=df_transformed, type_map=type_map, base_table_name="clean_data") 
    print("Data loading step completed for 'clean_data'.")

with DAG(
    dag_id="intelligent_etl_pipeline",
    default_args=default_args,
    description='An intelligent ETL pipeline with LightAutoML and Pydantic, loading to dynamic tables.',
    start_date=datetime(2025, 1, 1), # Adjust start_date as needed
    schedule_interval="@daily", # Or None, or cron expression
    catchup=False,
    tags=['etl', 'intelligent', 'dynamic_tables'],
) as dag:
    run_etl_task = PythonOperator(
        task_id="run_complete_etl_dynamic_tables", # Updated task_id for clarity
        python_callable=run_pipeline,
    )
