import os
import json
import boto3
import pandas as pd

def transform_carts(bucket_name, carts_file):
    """
    Reads JSON carts data from S3 bucket, normalizes it by flattening
    nested fields, save it locally in a csv, and returns the file path.

    Args:
        carts_file (str): Path to the JSON file containing carts data.

    Returns:
        str: Path to the file where the transformed data is saved.

    """
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=carts_file)
    file_content = response['Body'].read().decode('utf-8')

    # Parse JSON content and normalize the data into a DataFrame
    data = json.loads(file_content)

    carts_data = []
    for cart in data['carts']:
        cart_id = cart['id']
        user_id = cart['userId']
        total_cart_value = cart['total']

        for product in cart['products']:
            product_id = product['id']
            quantity = product['quantity']
            price = product['price']

            carts_data.append(
            {
                'cart_id': cart_id,
                'user_id': user_id,
                'product_id': product_id,
                'quantity': quantity,
                'price': price,
                'total_cart_value': total_cart_value
            })
    df = pd.DataFrame(carts_data)

    output_file_path = f"clean_data/{carts_file.split('/')[-1].split('.')[0]}/transformed.csv"

    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

    df.to_csv(output_file_path, index=False)

    # Return the path to the saved file
    return output_file_path