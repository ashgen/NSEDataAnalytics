import MySQLdb
from dateutil.parser import parse
import datetime
import time
from config import *
import numpy as np
from qpython import *
import csv

import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename='/tmp/kdb.log',
                    filemode='w')
demo_file="data_20151123_v1.csv"
with qconnection.QConnection(host=kdb_host,port=kdb_port) as qconn:
    demo_table="test"
    try:
        qconn.open()
        format=typekdb("%s"%demo_table,qconn)
        
        csvopen=csv.reader(file(demo_file))
        for row in csvopen:
            
            if "" in row:
                logging.error(':Length of row %s didnt match column format',",".join(row))
                continue
            logging.info('%s'%",".join(row)) 
            ''' `test1 insert (`$("HDIL-1M");"Z"$("20151123 094042");1800)'''
            data_res=insertkdb(format,row)
            
            print '`%s insert (%s)'%(demo_table,data_res)
            qconn('`%s insert (%s)'%(demo_table,data_res))
    except :
        raise
    finally:
        qconn.close()
        

                        
    



