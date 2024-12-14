from io import BytesIO
import requests
import boto3
import json

def fetch_data_to_s3(api_url, bucket_name):
    # Instatiate S3 client
    s3 = boto3.client('s3')

    # Fetch data from the API
    response = requests.get(api_url)

    if response.status_code == 200:
        data = response.json()

        json_data = json.dumps(data, indent=4)
        byte_data = json_data.encode('utf-8')
        file_obj = BytesIO(byte_data)

        # Upload the file to S3 in the 'raw' directory
        s3_key = f'raw/{api_url.split("/")[-1]}.json'
        s3.upload_fileobj(file_obj, bucket_name, s3_key)

        return s3_key
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")