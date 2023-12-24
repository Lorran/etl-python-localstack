#connect s3 bucket localstack and create bcuket
from typing import Any
import boto3
from botocore.exceptions import ClientError
import os
import pandas as pd

#class S3Bucket:
class S3Bucket:
    def __init__(self, aws_config: list[str], endpoint_url: str):
        self.__config = aws_config
        self.__endpoint_aws = endpoint_url
        #self.__region_aws = self.__config['default']['region']

    def get_list_files_in_bucket(self, bucket_name):
        s3_client = boto3.client('s3', endpoint_url=self.__endpoint_aws)
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        files = [file['Key'] for file in response['Contents']]
        return files

    def read_csv_from_bucket(self, bucket_name, file_name):
        s3_client = boto3.client('s3', endpoint_url=self.__endpoint_aws)
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        file_content = response['Body']
        df = pd.read_csv(file_content)
        return df
    
#read_csv_from_bucket and copy to other bucket

 
    def get_list_of_buckets_created(self):
        s3_client = boto3.client('s3', endpoint_url=self.__endpoint_aws)
        response = s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        return buckets
    
    def create_buckets(self, lst_bucket_names):
        s3_client = boto3.client('s3', endpoint_url=self.__endpoint_aws)
        for bucket_name in lst_bucket_names:
            try:
                s3_client.create_bucket(Bucket=bucket_name)
                print(f'Bucket {bucket_name} created successfully')
            except ClientError as e:
                print(e)
                print(f'Bucket {bucket_name} could not be created')
                return False
        return True

    def drop_files(self, lst_bucket_names):
        s3_client = boto3.client('s3', endpoint_url=self.__endpoint_aws)
        for bucket_name in lst_bucket_names:
            try:
                response = s3_client.list_objects_v2(Bucket=bucket_name)
                if 'Contents' in response:
                    for file in response['Contents']:
                        s3_client.delete_object(Bucket=bucket_name, Key=file['Key'])
            except ClientError as e:
                print(e)
                return False
        return True


    def drop_buckets(self, lst_bucket_names):
        s3_client = boto3.client('s3', endpoint_url=self.__endpoint_aws)
        for bucket_name in lst_bucket_names:
            try:
                s3_client.delete_bucket(Bucket=bucket_name)
            except ClientError as e:
                print(e)
                return False
        return True

    def upload_to_s3(self, file_name, bucket, s3_file_name):
        s3 = boto3.client('s3', endpoint_url=self.__endpoint_aws)
        try:
            s3.upload_file(file_name, bucket, s3_file_name)
            print("Upload Successful")
            return True
        except FileNotFoundError:
            print("The file was not found")
            return False
        except: 
            return False

    def upload_to_s3_string(self, csv_data, bucket_name, file_name):
        s3_resource = boto3.resource('s3', endpoint_url=self.__endpoint_aws)
        s3_resource.Object(bucket_name, file_name).put(Body=csv_data)

#Local File
    def list_files_in_raw_directory(self):
        directory_path = 'data/raw'
        folder_path = os.path.join(os.getcwd(), directory_path)

        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            files = os.listdir(folder_path)
            if len(files) > 0:
                #print("Files in directory '{}'".format(directory_path))
                # for file_name in files:
                #     print(file_name)
                #     return file_name
                return files
            else:
                print("No files found in '{}' directory.".format(directory_path))
        else:
            print("Directory '{}' does not exist or is not a valid directory.".format(directory_path))


# if __name__ == "__main__":
#     s3 = S3Bucket()
#     p1 = ('bucket-row-data','bucket-transform-data')
#     #lst_buckets = s3.create_buckets(p1)
#      s3.delete_buckets(lst_buckets)
#     print(s3.get_list_of_buckets_created())
 
