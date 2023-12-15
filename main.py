from datetime import datetime
from etl_scripts.manage_s3 import S3Bucket as S3
from dotenv import load_dotenv
import configparser
import os
from datetime import datetime


load_dotenv()
config = configparser.ConfigParser()
config.read('config/aws_config.ini')
endpoint_url_aws = os.getenv('endpoint_url')
bucket = S3(config, endpoint_url_aws)

now = datetime.now()
datetime_str = now.strftime("%Y%m%d%H%M%S")

bucket.create_buckets(['bucket-raw', 'bucket-processed'])
#print(bucket.get_list_of_buckets_created())
#print(bucket.get_list_of_buckets_created())

#bucket.drop_buckets(['bucket-raw', 'bucket-processed'])

path = 'data/raw/'
for file in bucket.list_files_in_raw_directory():
    file_name, file_extension = os.path.splitext(file)
    bucket.upload_to_s3(path + file, 'bucket-raw', file_name + '_' + datetime_str + file_extension)
