import config
from config import *
import pandas as pd
import numpy as np

query='select bid,ask,ticklast,volume from fut_one_day where symbol=`$("%s-1M")'
all_data=pd.read_csv("C:/Users/ashish/Desktop/workspace/data/fut_one_day-2016.01.07.csv")
def intraday_buy_sell(symbol):
    with qconnection.QConnection(host=kdb_host,port=kdb_port) as qconn:
        '''data=qconn(query%(symbol))
        data=pd.DataFrame.from_records(data)'''
        global all_data
        data=all_data[all_data.symbol==(symbol+"-1M")]
        bidvolume=data[data.ticklast<((data.bid+data.ask))/2].volume.sum()
        askvolume=data[data.ticklast>((data.bid+data.ask)/2)].volume.sum()
        liquidity=np.average((data.ask-data.bid)/((data.ask+data.bid)/2))
        return ((float(askvolume)/data.volume.sum())*100 if data.volume.sum()!=0 else 0,data.volume.sum(),liquidity )
if __name__=="__main__":
    all_buy=pd.DataFrame(columns=["SYMBOL","AGG","VOLUME","LIQUIDITY"])
    for symbol in get_symbols():
        agg,volume,liquidity=intraday_buy_sell(symbol)
        
        all_buy=all_buy.append({"SYMBOL":symbol,"AGG":agg,"VOLUME":volume,"LIQUIDITY":liquidity},ignore_index=True)
    
    candidates=all_buy.sort(['LIQUIDITY','VOLUME'],ascending=[True,False]).head(30)
    top_buy=candidates.sort('AGG',ascending=False).head(10)
    