import os
from airflow import DAG
from dotenv import load_dotenv
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

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

# Load env vars
load_dotenv()

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
    template_searchpath=[os.getenv('TEMPS_PATH')],
    schedule_interval="@daily",
    catchup=False
) as dag:

    # Dummy tasks to represent APIs
    fetch_user_data_api = DummyOperator(
        task_id="users_api_sensor"
    )

    fetch_prod_data_api = DummyOperator(
        task_id="products_api_sensor"
    )

    fetch_carts_data_api = DummyOperator(
        task_id="carts_api_sensor"
    )

    # Extract Data from APIs and upload to S3
    fetch_user_data = PythonOperator(
        task_id="extract_users_json_to_S3",
        python_callable=extract_users_data
    )

    fetch_prod_data = PythonOperator(
        task_id="extract_products_json_to_S3",
        python_callable=extract_products_data
    )

    fetch_carts_data = PythonOperator(
        task_id="extract_carts_json_to_S3",
        python_callable=extract_carts_data
    )

    # Transform Data
    create_users_datase = PythonOperator(
        task_id="create_users_datase",
        python_callable=transform_user_data,
        provide_context=True
    )

    create_products_datase = PythonOperator(
        task_id="create_products_datase",
        python_callable=transform_product_data,
        provide_context=True
    )

    transactions_dataset = PythonOperator(
        task_id="create_transactions_dataset",
        python_callable=transform_cart_data,
        provide_context=True
    )

    # Load Datasets to RDS Postgress
    load_users_to_db = PythonOperator(
        task_id="load_users_data_to_DB",
        python_callable=load_user_data,
        provide_context=True
    )

    load_products_to_db = PythonOperator(
        task_id="load_products_data_to_DB",
        python_callable=load_product_data,
        provide_context=True
    )

    load_carts_to_db = PythonOperator(
        task_id="load_transactions_data_to_DB",
        python_callable=load_cart_data,
        provide_context=True
    )

    # Database Operations
    user_transactions = PostgresOperator(
        task_id='user_transactions',
        postgres_conn_id='postgress_connection',
        sql='user_transactions.sql',
        dag=dag,
    )

    enriched_transactions = PostgresOperator(
        task_id='product_enriched_transactions',
        postgres_conn_id='postgress_connection',
        sql='enriched_transactions.sql',
        dag=dag,
    )

    # Database Operations
    user_summaries = PostgresOperator(
        task_id='user_transaction_summaries',
        postgres_conn_id='postgress_connection',
        sql='user_summaries.sql',
        dag=dag,
    )

    category_sales_summary = PostgresOperator(
        task_id='category_sales_summary',
        postgres_conn_id='postgress_connection',
        sql='category_sales_summary.sql',
        dag=dag,
    )
    
    cart_details = PostgresOperator(
        task_id='cart_details',
        postgres_conn_id='postgress_connection',
        sql='cart_details.sql',
        dag=dag,
    )

    # Set task dependencies
    fetch_user_data_api >> fetch_user_data >> create_users_datase
    fetch_carts_data >> create_users_datase
    fetch_prod_data_api >> fetch_prod_data >> create_products_datase
    fetch_carts_data >> create_products_datase
    fetch_carts_data_api >> fetch_carts_data >> transactions_dataset

    create_users_datase >> load_users_to_db
    create_products_datase >> load_products_to_db
    transactions_dataset >> load_carts_to_db

    load_users_to_db >> user_transactions
    load_carts_to_db >> user_transactions

    load_products_to_db >> enriched_transactions
    load_carts_to_db >> enriched_transactions

    user_transactions >> user_summaries
    user_transactions >> cart_details

    load_carts_to_db >> category_sales_summary
    load_products_to_db >> category_sales_summary