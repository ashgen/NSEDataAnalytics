import config
import ast
import socket
import sys
import MySQLdb
from dateutil.parser import parse
import datetime
import time
from config import *

import numpy as np

import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename='/tmp/kdb.log',
                    filemode='w')

demo_table="fut_one_day"
'''Query for first 4'''
query='10#`gapup %s gaplasttoopen:\
                select gapup:100*(last %s - first %s)%%\
                first %s  by symbol from fut_one_day where not %s=0'
''' Query for Last 2'''
query_1='10#`gapup %s gaplasttoopen:\
                select gapup:100*(last %s - last %s)%%\
                last %s  by symbol from fut_one_day where not %s=0'                
def __query__(xasc,compare,close):
    return query%(xasc,compare,close,close,close)

def __query_1__(xasc,compare,close):
    return query_1%(xasc,compare,close,close,close)

                
def __print__(res,msg=None):
    ''' new print menthodolgy for '''
    msg=''if(msg==None) else msg
    msg+=('-'*60)+"\n"
    for k,v in (zip([k[0][0] for k in res.items()],[k[1][0] for k in res.items()])):
            msg+= ("%20s\t%20f\n"%(k,v))
    msg+=('-'*60)+"\n"
    return msg        
                            
def OpenTOPreviousCloseTop10():
    with qconnection.QConnection(host=kdb_host,port=kdb_port) as qconn:
        qconn.open()
        res=qconn(__query__('xdesc','tickopen','tickclose'))
        qconn.close()
        return ("Highest Close to Open Change\n"+__print__(res))
        
        

def OpenTOPreviousCloseBottom10():
    with qconnection.QConnection(host=kdb_host,port=kdb_port) as qconn:
        qconn.open()
        res=qconn(__query__('xasc','tickopen','tickclose'))
        qconn.close()
        return ("Lowest Close to Open Change\n"+__print__(res))
        
        

def LastTOOpenTop10():
    with qconnection.QConnection(host=kdb_host,port=kdb_port) as qconn:
        qconn.open()
        res=qconn(__query__('xdesc','ticklast','tickopen'))
        qconn.close()
        return ("Highest Open to Current Price CHange\n"+__print__(res))
        

def LastTOOpenBottom10():
    with qconnection.QConnection(host=kdb_host,port=kdb_port) as qconn:
        qconn.open()
        res=qconn(__query__('xasc','ticklast','tickopen'))
        qconn.close()
        return ("Lowest Open to Current Price CHange\n"+__print__(res))

def LowToLastTop10():
    with qconnection.QConnection(host=kdb_host,port=kdb_port) as qconn:
        qconn.open()
        res=qconn(__query_1__('xdesc','ticklast','ticklow'))
        qconn.close()
        return ("Highest Low to Current Price CHange\n"+__print__(res))
        

def HighToLastBottom10():
    with qconnection.QConnection(host=kdb_host,port=kdb_port) as qconn:
        qconn.open()
        res=qconn(__query__('xasc','ticklast','tickhigh'))
        qconn.close()
        return ("Lowest High to Current Price CHange\n"+__print__(res))


def __sendmail__(msg):
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.starttls()
    server.login(config.__username__,config.__password__)
    server.sendmail(fromaddr, toaddrs, msg)
    server.quit()        
        
if __name__=='__main__':
    header="Subject:%s:Opening Day Analytics\n"%datetime.datetime.now()
    msg=header+OpenTOPreviousCloseTop10()+OpenTOPreviousCloseBottom10()+LastTOOpenBottom10()+LastTOOpenTop10()+LowToLastTop10()+HighToLastBottom10()
    print msg
    __sendmail__(msg)