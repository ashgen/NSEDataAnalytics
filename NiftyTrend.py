import pandas as pd
import MySQLdb
import config
from numpy import NaN
from pandas.stats.api import ols
import matplotlib.pyplot as plt
import sys
'''
first get the data,calculate previous close price,calcalate no of positive days and the distribution of the +ve
effect of open to the day close. Do the same for the -ve days.plot the distribution and as well as its statistics 
'''


def calc_positive_negative_dates(data,pos_x_min=0.005,pos_x_max=0.01,neg_y_max=-0.005,neg_y_min=-0.01):
    pdata=data[((((data.OPEN-data.PREV_CLOSE)/data.PREV_CLOSE)>pos_x_min) & (((data.OPEN-data.PREV_CLOSE)/data.PREV_CLOSE)<pos_x_max)) | ((((data.OPEN-data.PREV_CLOSE)/data.PREV_CLOSE)<neg_y_max) & (((data.OPEN-data.PREV_CLOSE)/data.PREV_CLOSE)>neg_y_min))]
    #calculate prev close to open return andn open to close return and regress
    prev_close_open=(pdata.OPEN-pdata.PREV_CLOSE)/pdata.PREV_CLOSE
    open_close=(pdata.CLOSE-pdata.OPEN)/pdata.OPEN
    fig=plt.figure()
    plt.scatter(x=prev_close_open, y=open_close)
    fig.suptitle('Posb(%03f,%03f)andNeg(%03f,%03f)'%(pos_x_min,pos_x_max,neg_y_max,neg_y_min),fontsize=20)
    plt.xlabel('prev_close_open',fontsize=10)
    plt.ylabel('open_close',fontsize=10)
    plt.savefig('Posb(%03f,%03f)andNeg(%03f,%03f).jpg'%(pos_x_min,pos_x_max,neg_y_max,neg_y_min))
    res=ols(y=open_close,x=prev_close_open)
    print res
if __name__=='__main__':
    data=config.get_symbol_future(sys.argv[1])
    calc_positive_negative_dates(data)    
    
        