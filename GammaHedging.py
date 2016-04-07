import MySQLdb
import config
import pandas as pd
import numpy as np

if __name__=='__main__':
    db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
    sql='select min(TIMESTAMP) as time,EXPIRY_DT from opt_greeks where symbol="NIFTY" and option_typ="CE""  group by EXPIRY_DT'