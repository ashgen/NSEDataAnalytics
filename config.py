'''
Created on Nov 23, 2015

@author: ashish
'''
import pandas as pd
from pandas.tseries.offsets import BDay
import datetime
from dateutil.parser import parse
import datetime
from qpython import *
import smtplib
import requests
import os
import zipfile
import MySQLdb
from numpy import NaN

global __username__,__password__
fromaddr = 'ashishsachan919@gmail.com'
toaddrs  = 'ashishsachan.iitkgp@gmail.com'
msg = 'There was a terrible error that occured and I wanted you to know!'
user_agent='Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.3) Gecko/2008092416 Firefox/3.0.3'
down_fir='C:/Users/ashish/Desktop/workspace/data/EOD/'
down_file_name='fo%sbhav.csv.zip'
vol_file_name='vol%s.csv'
equity_file_name='cm%sbhav.csv.zip'
nse_url='http://www.nseindia.com/content/historical/DERIVATIVES/%s/%s/%s'
equity_url='http://www.nseindia.com/content/historical/EQUITIES/%s/%s/%s'

vol_url='http://www.nseindia.com/archives/nsccl/volt/CMVOLT_%s.CSV'
wgt_comd='wget --user-agent="%s" -O %s %s'
table_hist='fut_opt_hist'
table_last='fut_opt_last'

# Credentials (if needed) for gmail
__username__='ashishsachan919'
__password__='*#goetia19#'

#Symbols table s not created 
symbols_table_not_created=['S&P500','BAJAJ-AUTO','L&TFH','M&M','M&MFIN']

#Table Connection
host="localhost"
user="nse"
password="Quant123"
database="nse_fut"
kdb_host="localhost"
kdb_port=5000
quandl_apikey='HxS_agfj9wNUpJTavCLo'
tests = [
# (Type, Test)
(int, int),
(float, float),
(datetime, parse)
]
def typekdb(table,qconn):
    
    try:
        
        return qconn('select t from meta %s'%table)['t']
    except:
        raise
                       
def type(str):
    for typ, test in tests:
        try:
            a=test(str)
            return typ
        except ValueError:
            continue
def get_format_sql(cursor,table):
    sql="SELECT data_type\
        FROM INFORMATION_SCHEMA.COLUMNS\
        WHERE\
        TABLE_NAME = '%s'\
        order by ordinal_position";
    return cursor.execute(sql%table);

def get_symbol_future(symbol):
    db=MySQLdb.connect(host,user,password,'NSE')
    sql='select symbol,timestamp,OPEN,HIGH,LOW,CLOSE,CONTRACTS from %s where MONTH_CODE="1M" and (instrument="FUTIDX" or instrument="FUTSTK")\
        order by timestamp '
    sql=sql%symbol
    try:
        data=pd.read_sql(sql,db)
    except :
        return pd.DataFrame()    
    data['PREV_CLOSE']=data.CLOSE.shift()
    data=data[data.PREV_CLOSE != NaN]
    return data
    
def insertkdb(format,row):
    
    return ";".join(["\"j\"$%s" % m if format[z]=='j' else "\"f\"$%s" % m \
                               if format[z]=='f' else  "\"Z\"$(\"%s\")" % parse(m).strftime("%Y%m%d %H%M%S") \
                               if format[z]=='z' else "`$(\"%s\")" % m for (m,z) in (zip(row[:1],range(0,len(format)))\
                               if hasattr(row, "__iter__") else zip(row.split(",")[:-1],range(0,len(format))))])
    
def insertsql(data,format=None):
    if format==None:
        raise Exception
    else:
        return  [int(m) if format(m)=="int" else float(m) \
                 if format(m)=="float" else parse(m).strftime("%Y-%m-%d %H:%M:%S") \
                 if type(m)=="datetime" else m for (m,z) in \
                 ( zip(data.split(","),range(0,len(format) ) )if hasattr(row, "__iter__")else zip(data,range(0,len(format))) )]

def get_symbols():
    db=MySQLdb.connect(host,user,password,'NSE')
    sql='select distinct symbol from fut_opt_last'
    symbols=pd.read_sql_query(sql,db)
    if symbols.empty:
        sql='select distinct symbol from fut_opt_hist'
        symbols=pd.read_sql_query(sql,db)
    return symbols.symbol        

def get_option_static_data_last_day(lastBDay):
    global user_agent,down_file_name,down_fir,nse_url,wgt_comd
    
    #lastBDay=today -BDay(1)
    day=str('%02d'% lastBDay.day)
    month=str.upper(lastBDay.strftime('%b'))
    year=str(lastBDay.year)
    form=day+month+year
    down_file=down_file_name%form
    _nse_url=nse_url%(year,month,down_file)
    _wgt_comd=wgt_comd % (user_agent,down_fir+down_file,_nse_url)
    os.system(_wgt_comd)
    
    try:
        file=zipfile.ZipFile(down_fir+down_file)
    except (IOError,zipfile.BadZipfile):
        print lastBDay.__str__()+"way a holiday\n"
        return None
    
    data=pd.read_csv(file.open(file.namelist()[0]))
    data.drop(data.columns[len(data.columns)-1], axis=1, inplace=True)
    '''Replace the date string in the dataframe to datetime object pandas EXPIRY_DT and TIMESTAMP'''
    data['EXPIRY_DT']=pd.to_datetime(data['EXPIRY_DT'])
    data['TIMESTAMP']=pd.to_datetime(data['TIMESTAMP'])
    return data

def get_vol_data_last_day(lastBDay):
    global user_agent,down_file_name,down_fir,nse_url,wgt_comd
    
    #lastBDay=today -BDay(1)
    form=lastBDay.strftime('%d%m%Y')
    vol_file=vol_file_name%form
    _vol_url=vol_url%(form)
    _wgt_comd=wgt_comd % (user_agent,down_fir+vol_file,_vol_url)
    os.system(_wgt_comd)
    
    try:  
        data=pd.read_csv(down_fir+vol_file)
        data.Date=pd.to_datetime(data.Date)
        data.drop(data.columns[[2,3,4,5,6]], axis=1, inplace=True)
        data.columns=['DATE','SYMBOL','VOLATILITY']
    except ValueError:
        return None
    
    return data

def get_vol_opt_hist(no_of_days):
    today = pd.datetime.today()
    for i in range(1,no_of_days):
        lastBDay=today-BDay(i)
        data=get_vol_data_last_day(lastBDay)
        if data is None:
            continue
        db=MySQLdb.connect(host,user,password,'NSE')
        data.to_sql('VOL_HIST',db ,flavor='mysql', if_exists='append', chunksize=200)
        db.close()
        
def get_fut_opt_hist(no_of_days):
    today = pd.datetime.today()
    for i in range(1,no_of_days):
        lastBDay=today-BDay(i)
        data=get_option_static_data_last_day(lastBDay)
        if data is None:
            continue
        db=MySQLdb.connect(host,user,password,'NSE')
        data.to_sql('FUT_OPT_HIST',db ,flavor='mysql', if_exists='append', chunksize=200)
        db.close()
def update_all_tables():
    symbols=get_symbols()
    for symbol in symbols:
        if symbol in symbols_table_not_created:
            continue
        try:
            db=MySQLdb.connect(host,user,password,'NSE')
            print "Updating the symbol "+symbol
            cursor=db.cursor()
            cursor.execute('delete from %s where TIMESTAMP=(select distinct TIMESTAMP from fut_opt_last)'%symbol)
            cursor.execute('insert into %s(INSTRUMENT, SYMBOL, EXPIRY_DT, STRIKE_PR, OPTION_TYP, OPEN, HIGH, LOW, CLOSE, SETTLE_PR, CONTRACTS, VAL_INLAKH, OPEN_INT, CHG_IN_OI, TIMESTAMP)\
                            (select INSTRUMENT, SYMBOL, EXPIRY_DT, STRIKE_PR, OPTION_TYP, OPEN, HIGH, LOW, CLOSE, SETTLE_PR, CONTRACTS, VAL_INLAKH, OPEN_INT, CHG_IN_OI, TIMESTAMP from fut_opt_last\
                            where symbol="%s")'%(symbol,symbol))
            db.commit()
            cursor.execute('update %s a,(select EXPIRY_DT,TIMESTAMP from fut_opt_last where INSTRUMENT="FUTIDX"  group by TIMESTAMP order by TIMESTAMP,EXPIRY_DT ) \
                            b set a.MONTH_CODE="1M" where a.EXPIRY_DT=b.EXPIRY_DT and a.TIMESTAMP=b.TIMESTAMP'%(symbol))
            db.commit()
            db.close()
        except ValueError:
            continue
        

    
    
if __name__=='__main__':
    '''
    db=MySQLdb.connect(host,user,password,'NSE')
    today = pd.datetime.today()
    data=get_option_static_data_last_day(today)
    #set_month_code(data)
    data.to_sql('FUT_OPT_LAST',db ,flavor='mysql', if_exists='replace', chunksize=200)
    '''
    get_vol_opt_hist(250)
    #lastBday=datetime.datetime.strptime('25122015','%d%m%Y')
    #get_vol_data_last_day(lastBday)
    '''
    update_all_tables()
    
    db.close()
    '''
    #get_fut_opt_hist(200)