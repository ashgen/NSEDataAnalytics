import MySQLdb
import pandas as pd
import config
import matplotlib.pyplot as plt
import sys
import math
if __name__=="__main__":
    symbol=sys.argv[1]
    db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
    '''Also add the Volume and the underlying Stock price '''
    sql='select TIME,ATM_VOL,SMILE,SKEW,SETTLE_PR,CONTRACTS from ATM_SKEW_SMILE_HIST a,%s b\
        where a.TIME=b.TIMESTAMP and\
        a.symbol="%s" and (b.INSTRUMENT="FUTIDX" or b.INSTRUMENT="FUTSTK") and b.MONTH_CODE="1M" order by TIMESTAMP desc limit 10'
    
    
    data=pd.read_sql(sql%(symbol,symbol),db)
    db.close()
    data=data.set_index('TIME')
    data["HIST_VOL"]=pd.rolling_std(data.SETTLE_PR.pct_change(),window=22)*100*math.sqrt((252/22))
    data.plot(subplots=True,)
    plt.show()