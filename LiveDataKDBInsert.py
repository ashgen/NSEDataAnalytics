import imp
import config
import ast
import socket
import sys
import MySQLdb
from dateutil.parser import parse
import datetime
import time
from config import *
import re
import numpy as np

#Table Connection
db=MySQLdb.connect(config.host,config.user,config.password,config.database)
cursor=db.cursor()

cutoff_time=datetime.datetime.strptime('15:31:00','%H:%M:%S').time()

# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Bind the socket to the portabc
server_address = ('localhost', 3000)
print >>sys.stderr, 'starting up on %s port %s' % server_address
sock.bind(server_address)
# Listen for incoming connections
sock.listen(1)
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename='/tmp/kdb.log',
                    filemode='w')
#demo_file="data_20151123_v1.csv"
demo_table="fut_one_day"


def __process_end_of_day__(qconn):
    ''' Process End of the day save the database in csv file format'''
    qconn('(`$":C:/Users/ashish/Desktop/workspace/data/fut_one_day","-",(string(.z.d)),".csv") 0:.h.tx[`csv;fut_one_day]')
    
with qconnection.QConnection(host=kdb_host,port=kdb_port) as qconn:
    qconn.open()
    format=typekdb("%s"%demo_table,qconn)
    
    ''' Removes the previous day data'''
    try:
        qconn("%s:([];symbol:`symbol$();time:`datetime$();tickopen:\
        `float$();tickhigh:`float$();ticklow:`float$();tickclose: `float$();ask:`float$();\
         askqty:`long$();bid:`float$();bidqty:`long$();ticklast:`float$();volume:`long$()) "%demo_table)
        
    except qconn.exception as e:
        raise
    
    while True:
        # Wait for a connection
        print >>sys.stderr, 'waiting for a connection'
        connection, client_address = sock.accept()
        sock.settimeout(5)
        try:
            print >>sys.stderr, 'connection from', client_address
    
            # Receive the data in small chunks and retransmit it
            while True:
                curr_time=datetime.datetime.time(datetime.datetime.now())
                '''
                if curr_time > cutoff_time:
                    __process_end_of_day__(qconn)
                    qconn.close()
                    exit(0)
                 ''' 
    
                fnoHead = connection.recv(3)
                if fnoHead == "FNO":
                    print >>sys.stderr, 'FNO'
                    dataLen = connection.recv(3)
                    dataLen=re.sub("[^0-9]","", dataLen)
                    val = ast.literal_eval(dataLen)
                    #print >>sys.stderr,"%s\n" % val.dtype
                    print >>sys.stderr, '%d\n' % val
                    data = connection.recv(val)
                    print >>sys.stdout, '%s\n' % data
                    if "" in data.split(","):
                        logging.error(':Length of row %s didnt match column format',data)
                        continue
                    logging.info('%s'%",".join(data)) 
                    ''' `test1 insert (`$("HDIL-1M");"Z"$("20151123 094042");1800)'''
                    data_res=insertkdb(format,data)
            
                    print '`%s insert (%s)'%(demo_table,data_res)
                    qconn('`%s insert (%s)'%(demo_table,data_res))
                    
                else:
                    print >>sys.stderr, 'FOI %s\n' % fnoHead
                    #dataLen = connection.recv(3)
                    dataLen = connection.recv(2)
                    val = ast.literal_eval(dataLen)
                    print >>sys.stderr, '%d\n' % val
                    data = connection.recv(val)
                    print >>sys.stderr, '%s\n' % data
        except socket.timeout:
            __process_end_of_day__(qconn)
            qconn.close()
            connection.close()
            db.close()
        finally:
            # Clean up the connection
            qconn.close()
            connection.close()
            db.close()


