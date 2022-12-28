import pandas as pd
from sqlalchemy import create_engine
import pyodbc
import logging
import os
import time
from datetime import datetime
import threading
import boto3
import json
import sys

class ProgressPercentage(object):
    def __init__(self, filename, file):
        self._filename = filename
        if os.path.exists(filename): 
            _size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

        self.created = datetime.now()
        self.perc = 2


    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100

            seconds = (datetime.now()-self.created).total_seconds()
            seconds_to_go = (self._size * seconds) / self._seen_so_far
            upload_MByte = self._seen_so_far/1000000/seconds
            upload_Mbit = (self._seen_so_far/1000000/seconds)*8

            if round((self._seen_so_far / self._size) * 100,2) % self.perc ==0:
            
                sys.stdout.write('\rupload - ' + self.filename_short + ' : ' + '{:.2f}'.format(percentage) + '% ({:.2f}'.format(upload_MByte) + ' MB/s / {:.2f}'.format(upload_Mbit) + ' Mb/s)' \
                                    + ' | {:.2f}'.format(self._seen_so_far/1000000) + ' MB / {:.2f}'.format(self._size/1000000) + ' MB' \
                                    + ' | ' + time.strftime("%H:%M:%S", time.gmtime(seconds_to_go - seconds)) + ' / ' + time.strftime("%H:%M:%S", time.gmtime(seconds_to_go)) \
                                )
                sys.stdout.flush()

def __export_upload_s3 (path, file):
    try:
        ACCESS_KEY  = 'INSERT_ACCESS_KEY'
        SECRET_KEY  = 'INSERT_SECRET_KEY'
        END_POINT   = 'INSERT_END_POINT'

        s3 = boto3.resource(
                                's3',
                                endpoint_url=END_POINT,
                                aws_access_key_id=ACCESS_KEY,
                                aws_secret_access_key=SECRET_KEY
                            )

        s3.Bucket('INSERT_BUCKET').upload_file(
                                                Filename=path
                                                , Key=file
                                                , Callback = ProgressPercentage(path,file)
                                                , ExtraArgs={'ACL':'public-read'}
                                            )

        print ('\n')
    except Exception as e:
        logging.critical(print(e))

def engine():
    
    engine = create_engine("mssql+pyodbc://USERNAME:PASSWORD@DATABASE_SERVER/DATABASE?driver=ODBC+Driver+17+for+SQL+Server")
    
    return engine

def convertDataFrameToParquet(location, tableName, table_df):
    
    logging.info('Converting to parquet...')
    path_parquet = location + '\\tmp\\' + tableName + '.parquet'
    print (path_parquet)
    table_df.to_parquet(path_parquet)
    logging.info(str(tableName)+'.parquet file created')
    

def extractTransformSend():
    try:
        
        #Setting the right current path of the script
        __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
        
        #Setting the right path of the JSON file
        if getattr(sys, 'frozen', False):
            f = open(file=os.path.join(sys._MEIPASS, "querries.json"))
        else:
            f = open(__location__+'\\querries.json')
        
        #Loading the JSON data    
        data = json.load(f)

        for i in data['tables']:
            
            logging.info('Reading table...')
            table_df = pd.read_sql(i['querry'],engine())
            
            convertDataFrameToParquet(__location__, i['name'], table_df)
            
            logging.info('Sending '+ str(i['name'])+'.parquet to S3Bucket...')
            path_parquet = __location__ + '\\tmp\\' + i['name'] + '.parquet'
            __export_upload_s3(path_parquet, i['name'])
            logging.info('File has been sent to S3 Bucket')
            
    except Exception as e:
        logging.critical(print(e))
    
    
        
        
        
        
if __name__ == '__main__':
    
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    #logging.basicConfig(filename='log.log', filemode='a', encoding='utf-8', level=logging.DEBUG)
    
    extractTransformSend()