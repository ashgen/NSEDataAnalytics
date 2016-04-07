'''
Created on Nov 23, 2015

@author: ashish
'''
import imp
#imp.load_source("config","/cygdrive/c/Users/ashish/Desktop/workspace/NSEDataAnalytics/neoPath/config.py")

import MySQLdb
from dateutil.parser import parse
import datetime
import time
import csv
import config
from config import type
from config import tests


def insert_into_database(File,database,single_or_many):
    db=MySQLdb.connect(config.host,config.user,config.password,config.database)
    cursor=db.cursor()
    try:
        csv_data=csv.reader(file(File))

        format=",".join(["%s" for m in range(0,len(csv_data.next()))])
        if single_or_many:
            for row in csv_data:
                data=[int(m) if type(m)==int else float(m) if type(m)==float else parse(m).strftime("%Y-%m-%d %H:%M:%S") if type(m)==datetime else m for m in row]

                try:
                    cursor.execute("insert into %s values(%s)"%(database,format),data)
                    db.commit()
                except MySQLdb.Error as e:
                    raise
        else:
            data=list(csv_data)

            try:
                format=",".join(["%s" for m in range(0,len(data[0]))])

                cursor.executemany("insert into %s values(%s)"%(database,format),data)
                db.commit()
            except MySQLdb.Error as e:
                raise

    except IOError:
        raise
    finally:
        db.close()

if __name__ == '__main__':
    File=r"C:\Users\ashish\Downloads\NeoFeedPlus (1)\data_20151123_v1.csv"
    database="test"
    insert_into_database(File,database,1)
