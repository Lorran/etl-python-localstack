from datetime import datetime
from etl_scripts.manage_s3 import S3Bucket as S3
from dotenv import load_dotenv
import configparser
import os


load_dotenv()
config = configparser.ConfigParser()
config.read('config/aws_config.ini')
endpoint_url_aws = os.getenv('endpoint_url')
bucket = S3(config, endpoint_url_aws)

#bucket.create_buckets(['bucket-raw', 'bucket-processed'])
#print(bucket.get_list_of_buckets_created())
#print(bucket.list_files_in_raw_directory())

#for file in bucket.list_files_in_raw_directory():
#    bucket.upload_file_to_s3_bucket(file_name= 'data/raw/Real_Estate_Sales_2001-2020_GL.csv' , bucket='bucket-raw')



bucket.upload_to_s3('data/raw/Real_Estate_Sales_2001-2020_GL.csv', 'bucket-raw', 'Real_Estate_Sales_2001-2020_GL.csv')
