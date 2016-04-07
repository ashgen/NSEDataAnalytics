import config
import ast
import socket
import sys
import MySQLdb
from dateutil.parser import parse
import datetime
import time
from config import *
import pandas as pd
import numpy as np
import re
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename='/tmp/kdb.log',
                    filemode='w')
symbol_file=r'C:\Neotrade\static_symbols.txt'
symbols=pd.read_csv(symbol_file,header=None).values.flatten()
'''First Get the symbol List for all the symbols.
    Then calculate the vwap for 2 different indicators and group by the decending or ascending order 
    sMA,sMA,lMA,symbol'''
query="select last t1.time,last t1.ticklast,last t1.vwap50,last t1.vwap20,diff:100*(last vwap20-last vwap50)%% last vwap50 from \
        t1:select time,ticklast,vwap50:sum(ticklast*volume)%% (sum volume),vwap20:sum(-%d#(ticklast*volume))%% (sum -%d#volume) from \
        t:-%d#select time,symbol,ticklast,volume from fut_one_day where symbol=`$(\"%s\")"
queryEMA="select last t1.time,last t1.ticklast,last t1.emavwap50,last t1.emavwap20,diff:100*(last emavwap20-last emavwap50)%% last emavwap50 from \
        t1:select time,ticklast,emavwap50:last ema[%f;ticklast*volume]%% (ema[%f;volume]),emavwap20:last ema[%f;-%d#(ticklast*volume)]%% ema[%f;-%d#volume] from \
        t:-%d#select time,symbol,ticklast,volume from fut_one_day where symbol=`$(\"%s\")"
queryVWAP="select last t1.time,last t1.ticklast,last t1.vwap,diff:100*(last ticklast-last vwap)%% last vwap,last liquidity from \
        t1:select time,ticklast,vwap:sum(ticklast*volume)%% (sum volume),liquidity:avg((ask-bid)%%((ask+bid)%%2))*100 from \
        t:select time,symbol,ticklast,volume,ask,bid from fut_one_day where symbol=`$(\"%s\")"    
                    
def MACD(sMA,lMA,symbol):
    with qconnection.QConnection(host=kdb_host,port=kdb_port) as qconn:
        return qconn(query%(sMA,sMA,lMA,symbol))

def EMACD(weight,sMA,lMA,symbol):
    with qconnection.QConnection(host=kdb_host,port=kdb_port) as qconn:
        return qconn(queryEMA%(weight,weight,weight,sMA,weight,sMA,lMA,symbol))

def VWAP(symbol):
    with qconnection.QConnection(host=kdb_host,port=kdb_port) as qconn:
        return qconn(queryVWAP%(symbol))

def MACD():
    sMA=50
    lMA=500
    weight=0.8
    for symbol in symbols:
        
        with qconnection.QConnection(host=kdb_host,port=kdb_port) as qconn:
            data=MACD(sMA, lMA, symbol)
            if 'all_data' in vars() or 'all_data' in globals(): 
                all_data=all_data.append(pd.DataFrame.from_records(data))
                
            else:
                global all_data
                all_data=pd.DataFrame.from_records(data)
                    
            
    '''Sort the all_data according to diff get first 10 and last 10'''
    all_data['symbol']=symbols   
    all_data=all_data[np.isfinite(all_data['diff'])]         
    top10=all_data.sort('diff',ascending=False).head(10)
    bottom10=all_data.sort('diff').head(10)
    
    print "Top 10 moving average contenders are\n"
    print top10
    print "-"*60
    print "Bottom 10 moving average contenders are\n"
    print bottom10

def VWAPmain():
    for symbol in symbols:
        pattern="(CE|PE)$"
        if re.search(pattern,symbol) is not None:
            continue
        with qconnection.QConnection(host=kdb_host,port=kdb_port) as qconn:
            data=VWAP(symbol)
            if 'all_data' in vars() or 'all_data' in globals(): 
                all_data=all_data.append(pd.DataFrame.from_records(data))
                
            else:
                global all_data
                all_data=pd.DataFrame.from_records(data)
                    
            
    '''Sort the all_data according to diff get first 10 and last 10'''
    all_data['symbol']=symbols   
    all_data=all_data[np.isfinite(all_data['diff'])]
    top10=all_data[all_data['diff']>0.5]
    bottom10=all_data[all_data['diff']<-0.5]         
    top10=top10.sort('liquidity',ascending=True).head(10)
    bottom10=bottom10.sort('liquidity',ascending=True).head(10)
    
    print "Top 10 moving average contenders are\n"
    print top10
    print "-"*60
    print "Bottom 10 moving average contenders are\n"
    print bottom10
            
if __name__=='__main__':
    VWAPmain()
        