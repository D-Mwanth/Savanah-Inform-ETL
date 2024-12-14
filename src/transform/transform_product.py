import os
import json
import boto3
import pandas as pd

# Extract carts products and combine them with the products table
def extract_cart_products(carts_file, bucket_name):
    
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=carts_file)
    file_content = response['Body'].read().decode('utf-8')

    # Parse JSON content
    data = json.loads(file_content)

    untracked_products = []
    for cart in data['carts']:
        for product in cart['products']:
            id = product['id']
            title =  product['title'].strip()
            category = product['thumbnail'].split("/")[5].strip()
            brand = product['title'].strip()
            price = product['price']

            if price > 50:
                untracked_products.append(
                {
                    'id': id,
                    'title': title,
                    'category': category,
                    'brand': brand,
                    'price': price
                })
    df = pd.DataFrame(untracked_products)
    return df

# Products Table
def transform_products(bucket_name, products_file, carts_file):
    """
    Reads JSON products and carts data from S3 bucket, normalizes it products data by flattening
    nested fields, extracted untracked products from cart_json, combine them with
    products data, and final copy save locally in a csv, and returns the file path.

    Args:
        bucket_name: Name of the bucket hosting the data.
        products_file (str): Path to the JSON file containing products data.
        carts_file (str): Path to the JSON file containing carts data.

    Returns:
        str: Path to the file where the transformed data is saved.
    """
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=products_file)
    file_content = response['Body'].read().decode('utf-8')

    # Parse JSON content and normalize the data into a DataFrame
    data = json.loads(file_content)

    df = pd.json_normalize(data["products"])
    df = df[df['price'] > 50]

    df = df[["id", "title", "category", "brand", "price"]]

    # combine products with cart products and dedup
    combined_df = pd.concat([df, extract_cart_products(carts_file, bucket_name)],
                            ignore_index=True).drop_duplicates(subset=['id'])

    combined_df.columns = ["product_id", "name", "category", "brand", "price"]
    # Path to save data
    output_file_path = f"clean_data/{products_file.split('/')[-1].split('.')[0]}/transformed.csv"
    # Create dir if does not exist
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

    combined_df.to_csv(output_file_path, index=False)

    # Return the path to the saved file
    return output_file_path