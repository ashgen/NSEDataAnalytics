import pandas as pd
import MySQLdb
import numpy
import config
from config import get_symbols
from vollib.black_scholes.implied_volatility import implied_volatility
from vollib.black_scholes.greeks.analytical import delta,gamma,vega,theta,rho

pd.set_option('precision',4)
present_database='NSE'
present_table='fut_opt_last'
rate=0.1
def get_data_present(symbol):
    db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
    sql='select INSTRUMENT,symbol,EXPIRY_DT,STRIKE_PR,OPTION_TYP,SETTLE_PR,OPEN_INT,CHG_IN_OI,CONTRACTS,TIMESTAMP from %s \
        where symbol="%s" and EXPIRY_DT = (select min(EXPIRY_DT) from %s where symbol="%s") order by INSTRUMENT,STRIKE_PR,OPTION_TYP;'
    sql=sql%(present_table,symbol,present_table,symbol)
    data=pd.read_sql_query(sql,db)    
    db.close()
    return data
def unique_date(symbol):
    db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
    sql='select distinct timestamp from %s'
    sql=sql%symbol
    data=pd.read_sql_query(sql,db)
    db.close()
    return data
def get_data_history(symbol,time):
    db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
    sql='select INSTRUMENT,symbol,EXPIRY_DT,STRIKE_PR,OPTION_TYP,SETTLE_PR,OPEN_INT,CHG_IN_OI,CONTRACTS,TIMESTAMP from %s \
        where MONTH_CODE="1M" and TIMESTAMP=str_to_date("%s","%%Y-%%m-%%d") order by INSTRUMENT,STRIKE_PR,OPTION_TYP;'
    sql=sql%(symbol,time)
    data=pd.read_sql_query(sql,db)    
    db.close()
    return data

def filter_data(data):
    expiry_time=(data[data.OPTION_TYP=='XX'].EXPIRY_DT-data[data.OPTION_TYP=='XX'].TIMESTAMP)[0]
    expiry_time=expiry_time.total_seconds()/(365*24*60*60)
    fut=data[data.OPTION_TYP=='XX']['SETTLE_PR'][0]
    call=data[data.OPTION_TYP=='CE']
    if call.empty:
        return (pd.DataFrame(),pd.DataFrame(),pd.DataFrame())
    put=data[data.OPTION_TYP=='PE']
    #Filter out of the money call and put with 10 percent change in underlying
    filt_call=call[(call.STRIKE_PR>fut) & (call.STRIKE_PR<1.1*fut)]    
    filt_put=put[(put.STRIKE_PR>0.9*fut) & (put.STRIKE_PR<fut)]
    #Filter all those values where implicit price of the call/put is being invalidated
    filt_call=filt_call[filt_call.STRIKE_PR+filt_call.SETTLE_PR>fut]
    filt_put=filt_put[fut+filt_put.SETTLE_PR>filt_put.STRIKE_PR]
    option=filt_put.append(filt_call,ignore_index=True)
    return (expiry_time,fut,option)

def implied_vol_calc(exp,fut,option):
    global rate
    
    iv_fun=lambda x:implied_volatility(x['SETTLE_PR'],fut,x['STRIKE_PR'],exp,0.1,'c' if x.OPTION_TYP=='CE' else 'p')*100
    #vollib.black_scholes.greeks.numerical.delta(flag, S, K, t, r, sigma)
    option['VOLATILITY']=option.apply(iv_fun,axis=1)
    delta_fun=lambda x:delta(('c' if x.OPTION_TYP=='CE' else 'p'),fut,x['STRIKE_PR'],exp,0.1,x.VOLATILITY/100)
    gamma_fun=lambda x:gamma(('c' if x.OPTION_TYP=='CE' else 'p'),fut,x['STRIKE_PR'],exp,0.1,x.VOLATILITY/100)
    vega_fun=lambda x:vega(('c' if x.OPTION_TYP=='CE' else 'p'),fut,x['STRIKE_PR'],exp,0.1,x.VOLATILITY/100)
    theta_fun=lambda x:theta(('c' if x.OPTION_TYP=='CE' else 'p'),fut,x['STRIKE_PR'],exp,0.1,x.VOLATILITY/100)
    rho_fun=lambda x:rho(('c' if x.OPTION_TYP=='CE' else 'p'),fut,x['STRIKE_PR'],exp,0.1,x.VOLATILITY/100)
    option['DELTA']=option.apply(delta_fun,axis=1)
    option['GAMMA']=option.apply(gamma_fun,axis=1)
    option['VEGA']=option.apply(vega_fun,axis=1)
    option['THETA']=option.apply(theta_fun,axis=1)
    option['RHO']=option.apply(rho_fun,axis=1)
    #Remove option with delta <0.05 for calls and delta >-0.05 for puts
    option=option[(option.DELTA>0.05) | (option.DELTA<-0.05)]
    #Remove where no contracts are being traded
    option=option[option.CONTRACTS!=0]
    option['FUT']=fut
    
    return option

def save_to_sql(option,symbol):
   db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
   cursor=db.cursor()
   cursor.execute('delete from OPT_GREEKS where timestamp=curdate() and symbol="%s"'%symbol)
   option.to_sql('OPT_GREEKS',db ,flavor='mysql', if_exists='append', chunksize=200)
   db.close()
def perform_calc_hist():
    symbols=get_symbols()
    for symbol in symbols:
        if symbol in config.symbols_table_not_created:
            continue
        dates=unique_date(symbol)
        for d in dates.timestamp:
            print symbol,d.__str__()+"\n"
            data=get_data_history(symbol,d)
            if data.empty:
                print symbol
                continue
            expiry,fut,option=filter_data(data)
            if option.empty:
                continue
            option=implied_vol_calc(expiry, fut, option)
            save_to_sql(option)
def perform_calc_present():
    symbols=get_symbols()
    for symbol in symbols:
        if symbol in config.symbols_table_not_created:
            continue
               
        print symbol.__str__()+"\n"
        data=get_data_present(symbol)
        if data.empty:
            print symbol
            continue
        expiry,fut,option=filter_data(data)
        if option.empty:
            continue
        option=implied_vol_calc(expiry, fut, option)
        save_to_sql(option,symbol)
        
if __name__=='__main__':
    perform_calc_present()

    
        