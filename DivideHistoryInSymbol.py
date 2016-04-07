from config import get_symbols
import config 
import MySQLdb
import pandas as pd
import datetime as dt
def divide_into_tables():
    symbols=get_symbols()
    sql='select INSTRUMENT, SYMBOL, EXPIRY_DT, STRIKE_PR, OPTION_TYP, OPEN, HIGH, LOW, CLOSE, SETTLE_PR, CONTRACTS, VAL_INLAKH, OPEN_INT, CHG_IN_OI, TIMESTAMP, MONTH_CODE from fut_opt_hist where symbol="%s"'
    
    for symbol in symbols.symbol:
        db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
        data=pd.read_sql_query(sql%symbol,db)
        try:
            data.to_sql(symbol,db ,flavor='mysql', if_exists='replace', chunksize=200)
        except ValueError:
            print symbol
            continue    
        db.close()    
def update_month_code():
    symbols=get_symbols()
    sql='update %s a,(select EXPIRY_DT,TIMESTAMP from fut_opt_hist where INSTRUMENT="FUTIDX"  group by TIMESTAMP order by TIMESTAMP,EXPIRY_DT ) b\
                            set a.MONTH_CODE="1M" where\
                            a.EXPIRY_DT=b.EXPIRY_DT and a.TIMESTAMP=b.TIMESTAMP'
    for symbol in symbols.symbol:
        print symbol
        db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
        cursor=db.cursor()
        try:
            cursor.execute(sql%(symbol))
        except :
            
            pass 
        finally:
            db.commit()
            db.close()    
def create_fut_history():
    db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
    
    try:
        for symbol in get_symbols():
            if symbol in config.symbols_table_not_created:
                continue
            print "Creating Future History for Symbol:"+symbol
            data=pd.read_sql('select symbol,open,high,low,close,CONTRACTS,OPEN_INT,TIMESTAMP from %s where OPTION_TYP="XX" and MONTH_CODE="1M" '%symbol,db)
            data.to_sql('FUT_HIST',db ,flavor='mysql', if_exists='append', chunksize=200)
    except:
        pass
    finally:
        db.close()
def create_fut_today():
    db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
    date=dt.date.today().__str__()
    try:
        print "Updating the Future History Table"
        data=pd.read_sql('select symbol,open,high,low,close,CONTRACTS,OPEN_INT,TIMESTAMP from fut_opt_last where OPTION_TYP="XX" and expiry_dt=(select max(expiry) \
        from (select min(EXPIRY_DT) as expiry from fut_opt_last group by symbol) a)',db)
        data.to_sql('FUT_HIST',db ,flavor='mysql', if_exists='append', chunksize=200)
    except:
        pass
    finally:
        db.close()
                
if __name__=='__main__':
    #divide_into_tables()
    #update_month_code()
    create_fut_today()