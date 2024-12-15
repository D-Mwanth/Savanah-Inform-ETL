from src.extract.extract_data_to_s3 import fetch_data_to_s3
from src.transform.transform_user import transform_users
from src.transform.transform_product import transform_products
from src.transform.transform_cart import transform_carts
from src.load.db_loader import load_csv_to_rds
from config.config import APIS, db_config, bucket_name

# Set data APIs
users_data_api = APIS["USERS_API"]
products_data_api = APIS["PRODUCTS_API"]
carts_data_api = APIS["CARTS_API"]

# Helper functions for executing function for extracting data from api
def extract_users_data():
    return fetch_data_to_s3(users_data_api, bucket_name)

def extract_products_data():
    return fetch_data_to_s3(products_data_api, bucket_name)

def extract_carts_data():
    return fetch_data_to_s3(carts_data_api, bucket_name)

# Helper function for cleaning and transforming data
def transform_user_data(**kwargs):
    users_s3_key = kwargs['ti'].xcom_pull(task_ids='extract_users_json_to_S3')
    carts_s3_key = kwargs['ti'].xcom_pull(task_ids='extract_carts_json_to_S3')
    return transform_users(bucket_name=bucket_name, users_file=users_s3_key, carts_file=carts_s3_key)

def transform_product_data(**kwargs):
    products_s3_key = kwargs['ti'].xcom_pull(task_ids='extract_products_json_to_S3')
    carts_s3_key = kwargs['ti'].xcom_pull(task_ids='extract_carts_json_to_S3')
    return transform_products(bucket_name=bucket_name, products_file=products_s3_key, carts_file=carts_s3_key)

def transform_cart_data(**kwargs):
    carts_s3_key = kwargs['ti'].xcom_pull(task_ids='extract_carts_json_to_S3')
    return transform_carts(bucket_name=bucket_name, carts_file=carts_s3_key)

#  Helper functions for executing db_load functions
def load_user_data(**kwargs):
    users_path = kwargs['ti'].xcom_pull(task_ids='create_users_datase')
    load_csv_to_rds(users_path, db_config, "users_table", "user_id")

def load_product_data(**kwargs):
    products_path = kwargs['ti'].xcom_pull(task_ids='create_products_datase')
    load_csv_to_rds(products_path, db_config, "products_table", "product_id")

def load_cart_data(**kwargs):
    carts_path = kwargs['ti'].xcom_pull(task_ids='create_transactions_dataset')
    load_csv_to_rds(carts_path, db_config, "carts_table", ["cart_id", "product_id"])