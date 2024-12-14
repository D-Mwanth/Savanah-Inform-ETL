## USERS
import os
import json
import boto3
import pandas as pd

def extract_carts_users(carts_file, bucket_name):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=carts_file)
    file_content = response['Body'].read().decode('utf-8')

    # Parse JSON content
    data = json.loads(file_content)

    carts_data = []
    for cart in data['carts']:
        carts_data.append({'user_id': cart['userId']})

    # Create the DataFrame and fill missing columns with NaN
    df = pd.DataFrame(carts_data, columns=["user_id", "first_name", "last_name", "gender", "age",
                                           "street", "city", "postal_code"])
    return df

def transform_users(bucket_name, users_file, carts_file):
    """
    Reads JSON users data from S3 bucket, normalizes it by flattening
    nested fields, save it locally in a csv, and returns the file path.

    Args:
        users_file (str): Path to the JSON file containing users data.

    Returns:
        str: Path to the file where the transformed data is saved.

    """
     # Instatiate S3 client
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=users_file)
    file_content = response['Body'].read().decode('utf-8')

    # Parse JSON content and normalize the data into a DataFrame
    data = json.loads(file_content)

    df = pd.json_normalize(data["users"])
    users_df = df[["id", "firstName", "lastName", "gender", "age",
                   "address.address", "address.city", "address.postalCode"]]
    users_df.columns = ["user_id", "first_name", "last_name", "gender", "age",
                        "street", "city", "postal_code"]

    df = pd.concat([users_df, extract_carts_users(carts_file, bucket_name)],
                   ignore_index=True).drop_duplicates(subset=['user_id'])

    output_file_path = f"clean_data/{users_file.split('/')[-1].split('.')[0]}/transformed.csv"

    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

    df.to_csv(output_file_path, index=False)

    # Return the path to the saved file
    return output_file_path