import MySQLdb
import pandas as pd
import config
import matplotlib.pyplot as plt
import sys
if __name__=='__main__':
    symbol=sys.argv[1]
    db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
    sql='select a.timestamp,a.close as indexpr,b.close as stockpr from fut_hist a inner join  fut_hist b on a.timestamp=b.timestamp\
     and a.symbol="NIFTY" and b.symbol="%s" order by a.timestamp'
    data=pd.read_sql(sql%symbol,db)
    data=data.set_index('timestamp')
    ret=data.pct_change()
    corr=pd.rolling_corr(ret.indexpr,ret.stockpr, window=20)
    data['CORR']=corr
    data.plot(subplots=True)
    plt.show() 
    
