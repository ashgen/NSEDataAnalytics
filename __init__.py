import pandas as pd
import config
import MySQLdb
from config import get_option_static_data_last_day,update_all_tables,get_vol_data_last_day
from numpy import NaN
from VolSmileCalc import perform_calc_present
from SplineInterpVol import perform_smile_skew_today
from GlobalData import update_global_data
from Result import get_result_data
from pandas.tseries.offsets import BDay
from CreateSymbolList import create_contract_list
from DivideHistoryInSymbol import create_fut_today
if __name__=='__main__':
    
    db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
    today = pd.datetime.today()
    data=get_option_static_data_last_day(today)
    #set_month_code(data)
    data.to_sql('FUT_OPT_LAST',db ,flavor='mysql', if_exists='replace', chunksize=200)
    data=get_vol_data_last_day(today)
    data.to_sql('VOL_HIST',db ,flavor='mysql', if_exists='append', chunksize=200)
    db.close()
    
    update_all_tables()
    
    perform_calc_present()
    perform_smile_skew_today()
    update_global_data()
    get_result_data()
    create_contract_list()
    create_fut_today()
    