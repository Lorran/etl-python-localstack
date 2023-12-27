from datetime import datetime
from etl_scripts.manage_s3 import S3Bucket as S3
from dotenv import load_dotenv
import configparser
import os
import pandas as pd
from io import StringIO
from etl_scripts.extract import Extract as Extract

load_dotenv()
config = configparser.ConfigParser()
config.read('config/aws_config.ini')
endpoint_url_aws = os.getenv('endpoint_url')
bucket = S3(config, endpoint_url_aws)
read = Extract()

now = datetime.now()
datetime_str = now.strftime("%Y%m%d%H%M%S")

# Drop existing buckets
bucket.drop_files(['bucket-raw', 'bucket-processed'])
bucket.drop_buckets(['bucket-raw', 'bucket-processed'])

# Create new buckets
bucket.create_buckets(['bucket-raw', 'bucket-processed'])

# Process files in the 'data/raw/' directory
path = 'data/raw/'
for file in bucket.list_files_in_raw_directory():
    # Upload file to s3
    file_name, file_extension = os.path.splitext(file)
    new_file_name = file_name + '_' + datetime_str + file_extension
    bucket.upload_to_s3(path + file, 'bucket-raw', new_file_name)

    # Extract data from file s3
    df = bucket.read_csv_from_bucket('bucket-raw', new_file_name)
    df_count = df.groupby('List Year').size().reset_index(name='Count')

    # Convert DataFrame to CSV and get CSV data as string
    csv_buffer = StringIO()
    df_count.to_csv(csv_buffer)
    csv_data = csv_buffer.getvalue()

    # Export DataFrame to CSV
    processed_file_name = file_name + '_process_' + datetime_str + '.csv'
    bucket.upload_to_s3_string(csv_data, 'bucket-processed', processed_file_name)

