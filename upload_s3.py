import os
import boto3
from botocore.exceptions import ClientError

def upload_folder_to_s3(city, city_folder, aoi, bucket = "wri-cities-heat"):
    """
    Recursively upload a local folder to an S3 bucket under the given prefix.
    """
    local_folder = f"data/{city_folder}"
    s3_prefix = f"{city}/scenarios/aoi/{aoi}"

    s3 = boto3.client("s3")
    local_folder = os.path.abspath(local_folder)


    for root, _, files in os.walk(local_folder):
        for filename in files:
            local_path = os.path.join(root, filename)
            # construct the relative path and then the S3 key
            relative_path = os.path.relpath(local_path, local_folder)
            s3_key = os.path.join(s3_prefix, relative_path).replace(os.sep, "/")

            try:
                s3.upload_file(local_path, bucket, s3_key)
                print(f"Uploaded {local_path} to s3://{bucket}/{s3_key}")
            except ClientError as e:
                print(f"Error uploading {local_path}: {e}")
