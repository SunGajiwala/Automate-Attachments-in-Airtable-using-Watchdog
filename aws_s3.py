import boto3
import os
import pandas as pd

# AWS credentials
aws_access_key_id = ''
aws_secret_access_key = ''

# Create an S3 client with the provided credentials
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

# Function to upload a file to S3
def upload_to_s3(file_path, bucket_name, s3_key):
    s3.upload_file(file_path, bucket_name, s3_key)

# Function to upload files from a DataFrame to S3
def upload_files_to_s3(dataframe, bucket_name):
    for index, row in dataframe.iterrows():
        file_path = row['Path']
        s3_key = f'docs/{row["Filename"]}'  # Prefix 'docs/' to the filename
        upload_to_s3(file_path, bucket_name, s3_key)
    print("Files uploaded Successfully to S3")

def delete_files_in_folder(bucket_name, folder_name):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)

    if 'Contents' in response:
        for obj in response['Contents']:
            s3_key = obj['Key']
            delete_from_s3(bucket_name, s3_key)
    else:
        print(f"No objects found in the folder {folder_name} of bucket {bucket_name}")

# Function to delete a file from S3
def delete_from_s3(bucket_name, s3_key):
    s3.delete_object(Bucket=bucket_name, Key=s3_key)
    print(f"File {s3_key} deleted from S3 bucket {bucket_name}")