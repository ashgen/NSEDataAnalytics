import MySQLdb
import pandas as pd
import numpy as np
import config
query='select concat_ws("",a.SYMBOL,cast(a.curyear as char),cast(a.curmonth as char),cast(a.STRIKE as char),cast(a.OPTION_TYP as char)) as CONTRACT from\
        (select SYMBOL,OPTION_TYP,RIGHT(YEAR(EXPIRY_DT),2) as curyear,LEFT(upper(monthname(EXPIRY_DT)),3) as curmonth,if(DELTA>0,min(STRIKE_PR),max(STRIKE_PR))\
        as STRIKE from opt_greeks where TIMESTAMP=(select max(TIMESTAMP) from opt_greeks)\
        and symbol in (select SYMBOL from nsesymbollist) group by SYMBOL,OPTION_TYP) a union\
        select concat(SYMBOL,"-1M") from nsesymbollist'
symbolsfile=r"C:/Neotrade/Symbol.txt"
def create_contract_list():
    db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
    stocks=pd.read_sql(query,db)
    stocks.to_csv(symbolsfile,index=False,header=False)
    db.close()
            
if __name__=='__main__':
    create_contract_list()
    
    