import configparser
import pandas as pd
import os
from datetime import datetime
#from manage_s3 import S3Bucket

#extract name of files tyope csv in folder data/raw/'

class Extract:
    def __init__(self):
        self.__config = configparser.ConfigParser()
        self.__config.read('config/etl_config.ini')

    def __get_path(self):
        path = self.__config['extract']['data_source_local']
        return path

    def get_list_file(self):
        path = self.__get_path()
        files = os.listdir(path)
        files_csv = [file for file in files if file.endswith('.csv')]
        return files_csv

    def extract_data_files(self, full_path_file):
        df = pd.read_csv(full_path_file)
        #df['Date Recorded'] = df['Date Recorded'].astype('datetime64[ns]')
        #grouped_data = df.groupby(['Date Recorded']).size().reset_index(name='Total')
        return df

    def move_file_after_extract(self, name_of_file):
        path_source = self.__config['extract']['data_source_local']
        path_target = self.__config['transform']['data_transform_local']
        base_name, extension = os.path.splitext(path_source+name_of_file)
        current_datetime   = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        print(current_datetime)
        #os.rename(path_source + name_of_file, path_target + base_name + '_' + current_datetime + extension)

#



    # config = configparser.ConfigParser()
    # config.read('config/etl_config.ini')
    # print(config['extract']['data_transform_local'])






    # df = pd.read_csv('data/raw/Real_Estate_Sales_2001-2020_GL.csv')
    # df['Date Recorded'] = df['Date Recorded'].astype('datetime64[ns]')
    # grouped_data = df.groupby(['Date Recorded']).size().reset_index(name='Total')
    # print(grouped_data)
