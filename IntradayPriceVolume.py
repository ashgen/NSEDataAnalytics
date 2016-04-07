import config 
from config import *
import MySQLdb
import matplotlib.pyplot as plt
import sys

query='select last ticklast,sum volume by 10 xbar time.minute from fut_one_day where symbol=`$("%s-1M")'

if __name__=="__main__":
    symbol=sys.argv[1]
    with qconnection.QConnection(host=kdb_host,port=kdb_port) as qconn:
        data=qconn(query%(symbol))
        data=pd.DataFrame.from_records(data)
        data.plot(subplots=True)
        plt.show()