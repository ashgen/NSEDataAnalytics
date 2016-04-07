from config import get_symbol_future,get_symbols
import pandas as pd

def calc_ema_slow_fast(data,sma,lma,symbol,threshold):
    
    if data.empty:
        return
    data['SMA']=pd.ewma(data.CLOSE,span=sma)
    data['LMA']=pd.ewma(data.CLOSE,span=lma)
    last_lema=data.LMA.values[-1]
    last_sema=data.SMA.values[-1]
    close=data.CLOSE.values[-1]
    buydiff=(last_sema-last_lema*(1+threshold))/close
    selldiff=(last_sema-last_lema*(1-threshold))/close
    if (buydiff>0):
        return ("Buy",buydiff)
    elif (selldiff<0):
        return ("Sell",selldiff)
    else:
        return ("Neutral",0)
    

if __name__=='__main__':
    sma=11
    lma=22
    threshold=0.02
    symbols=get_symbols() 
    result=pd.DataFrame()
    for symbol in symbols:
        data=get_symbol_future(symbol)
        if data.empty:
            continue
        (signal,strength)=calc_ema_slow_fast(data, sma, lma, symbol, threshold)
        #print symbol+"\t"+"Signal="+signal+"\n"
        result=result.append({'SYMBOL':symbol,'SIGNAL':signal,'STRENGTH':strength},ignore_index=True)
    result=result.sort(columns='STRENGTH',ascending=False)
    print "Top Buyers are"
    print result.head(5)
    print "Top Sellers are"
    print result.tail(5)