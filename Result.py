from config import wgt_comd,user_agent,down_fir
import config
import os
import pandas as pd
import MySQLdb
result_url='http://www.nseindia.com/corporates/datafiles/BM_Next_1_Month.csv'
def get_result_data():
    global result_url
    down_file='Result1M.csv'
    try:
        _wgt_comd=wgt_comd % (user_agent,down_fir+down_file,result_url)
        os.system(_wgt_comd)
        data=pd.read_csv(down_fir+down_file)
        db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
        data.to_sql('RESULTS', con=db, flavor='mysql',if_exists='replace', chunksize=200)
    except :
        pass
if __name__=='__main__':
    get_result_data()
    
    
    