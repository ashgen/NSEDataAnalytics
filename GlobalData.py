'''Create Correlation and other graph in for nifty with S&P,DAX,Chinese Markets,Gold,Crude Oil'''
import Quandl
import config
import datetime
import MySQLdb
import matplotlib.pyplot as plt
'''CNXNIFTY-YAHOO/INDEX_NSEI,FRED/DCOILBRENTEU-Oil,YAHOO/INDEX_GSPC-S&P500,YAHOO/INDEX_SSEC-China Cmposite Index,YAHOO/INDEX_GDAXI-DAX,LBMA/GOLD-Gold Prices'''
def update_global_data():
    data=Quandl.get(["YAHOO/INDEX_NSEI.4","FRED/DCOILBRENTEU.1","YAHOO/INDEX_GSPC.4","YAHOO/INDEX_SSEC.4","YAHOO/INDEX_GDAXI.4","LBMA/GOLD.1"],authtoken=config.quandl_apikey,trim_start='2001-01-01',returns='pandas')
    data.columns=['NIFTY','OIL','SP500','ChinaComp','DAX','GOLD']
    data=data.fillna(method='ffill')
    db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
    data.to_sql('MACRO', con=db, flavor='mysql', if_exists='replace', chunksize=200)
    '''Calcualte ratio Graphs'''
    
    data["NIFTY_OIL"]=data.NIFTY/data.OIL
    data["NIFTY_SP"]=data.NIFTY/data.SP500
    data["NIFTY_CHINA"]=data.NIFTY/data.ChinaComp
    data["NIFTY_DAX"]=data.NIFTY/data.DAX
    data["NIFTY_GOLD"]=data.NIFTY/data.GOLD
    return data
if __name__=='__main__':
    update_global_data()
    ratio=data[["NIFTY_OIL","NIFTY_SP","NIFTY_CHINA","NIFTY_DAX","NIFTY_GOLD"]]
    d=datetime.date.today() - datetime.timedelta(6*365/12)
    ratio=ratio[ratio.index>d.__str__()]
    ratio.plot(subplots=True)
    plt.show()