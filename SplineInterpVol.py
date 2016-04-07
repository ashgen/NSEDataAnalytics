import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.interpolate import splev,splrep
import MySQLdb
import config
from mpldatacursor import datacursor
import sys
from VolSmileCalc import unique_date
from datetime import date,datetime
from config import get_symbols,symbols_table_not_created
import warnings
warnings.filterwarnings("ignore")

opt_data_query='select TIMESTAMP,EXPIRY_DT,STRIKE_PR,FUT,VOLATILITY,OPTION_TYP,DELTA,CONTRACTS,OPEN_INT from opt_greeks\
         where symbol="%s"  and TIMESTAMP=STR_TO_DATE("%s","%%Y-%%m-%%d") order by STRIKE_PR'
last_date_query='select distinct timestamp from opt_greeks order by timestamp desc limit %d,1'
all_dates=sql='select distinct timestamp from %s'
def get_opt_vol_data(symbol,date):
    global sql
    sql=opt_data_query%(symbol,date)
    db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
    data=pd.read_sql(sql,db)
    db.close()
    call=data[data.OPTION_TYP=='CE']
    put=data[data.OPTION_TYP=='PE']
    put.DELTA=put.DELTA+1
    data=call.append(put)
    data=data.sort('DELTA')
    return data
def get_opt_vol_data_hist(symbol):
    
    db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
    
    points=pd.DataFrame()
    fig=plt.figure()
    for i in [0,1,5]:
        
        date=pd.read_sql(last_date_query%i,db)
        date=date.timestamp[0].__str__()
        data=get_opt_vol_data(symbol, date)
        (range,res,pol)=perform_spline_calc(data, xlow=0, xhigh=1, xsep=0.02)
        
        if i==0:
            points=data.DELTA
            ax1 = fig.add_subplot(111)
            ax2 = ax1.twiny()
            act,=ax1.plot(data.DELTA,data.VOLATILITY,'r*',label='Actual Smile')
            fit,=ax1.plot(range,res,'g',label='Fitted Smile')
            ax1.set_xlabel('DELTA')
            ax1.set_ylabel('VOLATILITY')
            
            #ax2.plot(data.STRIKE_PR,data.VOLATILITY,'b*',label='Actual Smile')
            ax2.set_xticks(data.DELTA)
            ax2.set_xticklabels(data.STRIKE_PR,rotation=90,fontsize=12)
            ax2.set_xlabel('STRIKE')
        elif i==1:
            ax1.plot(points,pol(points),'r',label='1 day ago')
        else:
            ax1.plot(points,pol(points),'b',label='7 days ago')
        lgd=ax1.legend(loc='upper left',bbox_to_anchor=(1,1))
        datacursor()
        fig.savefig("%s_VOL.jpg"%symbol, bbox_extra_artists=(lgd,), bbox_inches='tight')
            
                
        
    db.close()
def perform_smile_skew_today():
    db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
    cursor=db.cursor()
    today=date.today().__str__()
    symbols=get_symbols()
    skew_present_data=pd.DataFrame(columns=["TIME","SYMBOL","ATM_VOL","SKEW","SMILE","PUT_CALL_RATIO"])
    for symbol in symbols:
        print symbol,today
        data=get_opt_vol_data(symbol, today)
        if data.empty:
            continue 
        (atm_vol,skew,smile,put_call_ratio)=calculate_atm_skew_smile(data)
        skew_present_data=skew_present_data.append({"TIME":today,"SYMBOL":symbol,"ATM_VOL":atm_vol,"SKEW":skew,"SMILE":smile,"PUT_CALL_RATIO":put_call_ratio},ignore_index=1)
    cursor.execute('delete from ATM_SKEW_SMILE_HIST where TIME=CURDATE()')
    skew_present_data.to_sql("ATM_SKEW_SMILE_HIST",db,flavor='mysql', if_exists='append', chunksize=200)
    db.close()
    
def perform_smile_skew_hist(symbol):
    db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
    skew_hist_data=pd.DataFrame(columns=["TIME","SYMBOL","ATM_VOL","SKEW","SMILE","PUT_CALL_RATIO"])
    all_dates=unique_date(symbol)
    for date in all_dates.timestamp:
        data=get_opt_vol_data(symbol, date)
        if data.empty:
            continue 
        print symbol,date.__str__()
        (atm_vol,skew,smile,put_call_ratio)=calculate_atm_skew_smile(data)
        skew_hist_data=skew_hist_data.append({"TIME":date,"SYMBOL":symbol,"ATM_VOL":atm_vol,"SKEW":skew,"SMILE":smile,"PUT_CALL_RATIO":put_call_ratio},ignore_index=1)
    skew_hist_data.to_sql("ATM_SKEW_SMILE_HIST",db,flavor='mysql', if_exists='append', chunksize=200)
    db.close()
        
        
def calculate_atm_skew_smile(data):
    (range,res,pol)=perform_spline_calc(data)
    atm_vol=pol(0.5)
    skew=(pol(0.25)+pol(0.75))/(2*pol(0.5))
    smile=pol(0.25)-pol(0.75)
    call=data[data.OPTION_TYP=="CE"].OPEN_INT.sum()
    put=data[data.OPTION_TYP=="PE"].OPEN_INT.sum()
    put_call_ratio=(1.0*put)/(call) if call !=0 else 0
    return(atm_vol,skew,smile,put_call_ratio)
    
def perform_spline_calc(data,xlow=0,xhigh=1,xsep=0.02):
    range=np.arange(xlow,xhigh,xsep)
    fit=np.polyfit(data.DELTA,data.VOLATILITY, deg=2, w=data.CONTRACTS)
    pol= np.poly1d(fit)
    res=pol(range)
    #spline=splrep(data.DELTA,data.VOLATILITY, w=data.CONTRACTS, k=2)
    #res=splev(range,spline)
    return (range,res,pol)

def plot_iv(data,range,res,symbol):
    fig=plt.figure()
    ax1 = fig.add_subplot(111)
    ax2 = ax1.twiny()
    act,=ax1.plot(data.DELTA,data.VOLATILITY,'r*',label='Actual Smile')
    fit,=ax1.plot(range,res,'g',label='Fitted Smile')
    ax1.set_xlabel('DELTA')
    ax1.set_ylabel('VOLATILITY')
    #ax2.plot(data.STRIKE_PR,data.VOLATILITY,'b*',label='Actual Smile')
    ax2.set_xticks(data.DELTA)
    ax2.set_xticklabels(data.STRIKE_PR,rotation=90,fontsize=12)
    
    ax2.set_xlabel('STRIKE')
    #plt.show()
    datacursor()
    plt.savefig("%s_VOL.jpg"%symbol)
    
    
if __name__=="__main__":
    symbol=sys.argv[1]
    get_opt_vol_data_hist(symbol)
    '''
    symbols=get_symbols()
    for symbol in symbols: 
        if symbol in symbols_table_not_created:
            continue
        perform_smile_skew_hist(symbol)
    perform_smile_skew_today()'''
    
    
             