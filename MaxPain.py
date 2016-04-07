import config
import pandas as pd
import numpy
import MySQLdb
import sys
from numpy import nan
import warnings
warnings.filterwarnings("ignore")

def get_distinct_expiry_date(symbol):
    sql='select distinct EXPIRY_DT from %s where EXPIRY_DT<(select max(TIMESTAMP) from %s) order by EXPIRY_DT desc'
    db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
    expiry=pd.read_sql(sql%(symbol,symbol),db)
    db.close()
    return expiry.EXPIRY_DT
def get_data_expiry_day_1(symbol,expiry,days_till_expiry):
    sql='select STRIKE_PR,SETTLE_PR,OPEN_INT,OPTION_TYP from %s where TIMESTAMP=(select distinct TIMESTAMP from %s where STR_TO_DATE("%s","%%Y-%%m-%%d")>TIMESTAMP order by timestamp desc limit %d,1) \
        and (INSTRUMENT="OPTSTK" or INSTRUMENT="OPTIDX") and EXPIRY_DT=STR_TO_DATE("%s","%%Y-%%m-%%d")'
    db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
    data=pd.read_sql(sql%(symbol,symbol,expiry,days_till_expiry,expiry),db)
    db.close()
    return data
def get_fut_expiry_n_day(symbol,expiry,days_till_expiry):
    sql='select SETTLE_PR from %s where timestamp=(select distinct TIMESTAMP from %s where STR_TO_DATE("%s","%%Y-%%m-%%d")>TIMESTAMP order by timestamp desc limit %d,1)\
        and MONTH_CODE="1M" and (INSTRUMENT="FUTSTK" or INSTRUMENT="FUTIDX")'
    db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
    fut=pd.read_sql(sql%(symbol,symbol,expiry,days_till_expiry),db)
    db.close()
    return (nan if fut.empty else fut.SETTLE_PR.values[0])
    
def get_future_expiry(symbol,expiry):
    sql='select SETTLE_PR from %s where TIMESTAMP=STR_TO_DATE("%s","%%Y-%%m-%%d") and TIMESTAMP=EXPIRY_DT and (INSTRUMENT="FUTSTK" or INSTRUMENT="FUTIDX")'
    db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
    data=pd.read_sql(sql%(symbol,expiry),db)
    db.close()
    return data.SETTLE_PR
    
def max_pain(data):
    ''' Max Pain point can be calculated as follows
    for strike in strikes'''
    result=pd.DataFrame(columns=["STRIKE","MAX_PAIN"])
    for fut in data.STRIKE_PR.unique():
        
        fun=lambda x:((max(fut-x.STRIKE_PR,0)*x.OPEN_INT) if (x.OPTION_TYP=='CE') else (max(x.STRIKE_PR-fut,0)*x.OPEN_INT))
        res=data.apply(fun,axis=1)
        result=result.append({"STRIKE":fut,"MAX_PAIN":res.sum()},ignore_index=True)
    return result.ix[result.MAX_PAIN.idxmin()]
    
if __name__=='__main__':
    symbol=sys.argv[1]
    days_till_expiry=int(sys.argv[2])-1
    #days_till_expiry=2
    #symbol='AXISBANK'
    pain=pd.DataFrame(columns=["EXPIRY","FUT_EXPIRY","STRIKE","MAX_PAIN","DEVIATION","MONTH_RETURN","EXPIRY_RETURN"])
    expiries=get_distinct_expiry_date(symbol)
    for expiry in expiries:
        
        data=get_data_expiry_day_1(symbol, expiry,days_till_expiry)
        dem=max_pain(data)
        try:
            future=get_future_expiry(symbol, expiry).values[0]
        except IndexError:
            continue
        fut_30= get_fut_expiry_n_day(symbol, expiry,30)
        fut_2= get_fut_expiry_n_day(symbol, expiry,days_till_expiry)
        month_ret=100*(future-fut_30)/fut_30
        expiry_ret=100*(future-fut_2)/fut_2
        deviation=((future-dem.STRIKE)/future)*100
        pain=pain.append({"STRIKE":dem.STRIKE,"MAX_PAIN":dem.MAX_PAIN,"FUT_EXPIRY":future,"EXPIRY":expiry,"DEVIATION":deviation,"MONTH_RETURN":month_ret,"EXPIRY_RETURN":expiry_ret},ignore_index=True)
    pain.sort(columns, axis, ascending, inplace, kind, na_position)
    print pain
            
    
        
        
        