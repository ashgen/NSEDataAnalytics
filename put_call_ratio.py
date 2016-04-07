import MySQLdb
import pandas as pd
import config
import matplotlib.pyplot as plt
import sys
sql='select TIMESTAMP,FUT,TOTAL_OPEN_INT_PUT/TOTAL_OPEN_INT_CALL as PUT_CALL_OI_RATIO,TOTAL_CONTRACTS_PUT/TOTAL_CONTRACTS_CALL as PUT_CALL_VOLUME_RATIO\
        from (select TIMESTAMP,IF(OPTION_TYP="XX",SETTLE_PR,0) as FUT,sum(if(OPTION_TYP="CE",OPEN_INT,0)) as TOTAL_OPEN_INT_CALL,sum(if(OPTION_TYP="PE",OPEN_INT,0)) as TOTAL_OPEN_INT_PUT,\
        sum(if(OPTION_TYP="CE",CONTRACTS,0)) as TOTAL_CONTRACTS_CALL,sum(if(OPTION_TYP="PE",CONTRACTS,0)) as TOTAL_CONTRACTS_PUT\
        from %s  where month_code="1M" group by TIMESTAMP order by timestamp desc\
         ) a'
def get_put_call_ratio(symbol):
    db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
    data=pd.read_sql(sql%symbol,db)
    data=data.set_index('TIMESTAMP')
    return data
         
if __name__=='__main__':
    symbol=sys.argv[1]
    data=get_put_call_ratio(symbol)
    data.plot(subplots=True)
    plt.show()
    