
import os
from dotenv import load_dotenv

load_dotenv()

APIS = {
    "USERS_API": "https://dummyjson.com/users",
    "PRODUCTS_API": "https://dummyjson.com/products",
    "CARTS_API": "https://dummyjson.com/carts",
}

bucket_name = "savanah-inform"

db_config = {
    "host": os.getenv('DB_HOST'),
    "port": int(os.getenv('DB_PORT')),
    "database": os.getenv('DB_DATABASE'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD')
}