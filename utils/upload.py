import boto3
from botocore.exceptions import ClientError


def to_s3(file_path):
    """
    Upload a local file to an S3 bucket, maintaining the relative path from './data'.
    """
    # Create a session using a specific profile
    session = boto3.Session(profile_name='CitiesUserPermissionSet')
    s3 = session.client("s3")
    
    bucket="wri-cities-heat"
    s3_base_prefix="OpenUrban"

    s3_key = f"{s3_base_prefix}/{file_path}"

    print(f"Uploading {file_path} to s3://{bucket}/{s3_key}")
    try:
        s3.upload_file(file_path, bucket, s3_key)
        print(f"Success")
    except ClientError as e:
        print(f"  Error uploading {file_path}: {e}")
