import os
from airflow import DAG
from dotenv import load_dotenv
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from dag_utils.etl_helpers import (
    extract_users_data,
    extract_products_data,
    extract_carts_data,
    transform_user_data,
    transform_product_data,
    transform_cart_data,
    load_user_data,
    load_product_data,
    load_cart_data
)

# Load .env file
load_dotenv()

# AWS Access Credentials
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_DEFAULT_REGION')


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

# DAG definition
with DAG(
    "etl_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 12, 13),
    schedule_interval="@daily",
    catchup=False
) as dag:

    # Extract Data from APIs and upload to S3
    fetch_user_data = PythonOperator(
        task_id="Extract_User_Json_to_S3",
        python_callable=extract_users_data
    )

    fetch_prod_data = PythonOperator(
        task_id="Extract_Product_Json_to_S3",
        python_callable=extract_products_data
    )

    fetch_carts_data = PythonOperator(
        task_id="Extract_Cart_Json_to_S3",
        python_callable=extract_carts_data
    )

    # # Transform Data
    transform_users_data = PythonOperator(
        task_id="Transform_User_Data",
        python_callable=transform_user_data,
        provide_context=True
    )

    transform_products_data = PythonOperator(
        task_id="Transform_Product_Data",
        python_callable=transform_product_data,
        provide_context=True
    )

    transform_carts_data = PythonOperator(
        task_id="Transform_Cart_Data",
        python_callable=transform_cart_data,
        provide_context=True
    )

    # Load Datasets to RDS Postgress
    load_users_to_db = PythonOperator(
        task_id="Load_User_Data_to_DB",
        python_callable=load_user_data,
        provide_context=True
    )

    load_products_to_db = PythonOperator(
        task_id="Load_Product_Data_to_DB",
        python_callable=load_product_data,
        provide_context=True
    )

    load_carts_to_db = PythonOperator(
        task_id="Load_Cart_Data_to_DB",
        python_callable=load_cart_data,
        provide_context=True
    )

    # Set task dependencies
    fetch_user_data >> transform_users_data
    fetch_carts_data >> transform_users_data
    fetch_prod_data >> transform_products_data
    fetch_carts_data >> transform_products_data
    fetch_carts_data >> transform_carts_data

    transform_users_data >> load_users_to_db
    transform_products_data >> load_products_to_db
    transform_carts_data >> load_carts_to_db