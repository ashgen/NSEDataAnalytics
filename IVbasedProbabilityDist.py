''' The Entire idea is to evaluate the probablity distribution of a particular stock based on its out of the money options for the last day'''
import config
import numpy as np
from numpy.random import normal
from scipy.stats.kde import gaussian_kde
import matplotlib.pyplot as plt
import MySQLdb
from datetime import date,timedelta
from SplineInterpVol import get_opt_vol_data
from math import log,sqrt
from scipy.stats import norm
from pandas.tseries.offsets import BDay
import matplotlib.pyplot as plt
import sys
if __name__=='__main__':
    symbol=sys.argv[1]
    today=(date.today()-BDay(1)).__str__()
    data=get_opt_vol_data(symbol, today)
    d_fun=lambda x:(log(x.FUT/x.STRIKE_PR)-0.5*(x.VOLATILITY/100.0)*(x.VOLATILITY/100.0)*((x.EXPIRY_DT-x.TIMESTAMP).total_seconds()/(365*24*60*60)))/(0.01*x.VOLATILITY*sqrt((x.EXPIRY_DT-x.TIMESTAMP).total_seconds()/(365*24*60*60)))
    prob_fun=lambda x: norm.pdf(d_fun(x))
    data['PROBABLITY']=data.apply(prob_fun,axis=1)
    kde=gaussian_kde(data.PROBABLITY)
    #range=np.arange(-5,5,0.05)
    #hist=kde.evaluate(range)
    plt.plot(data.STRIKE_PR,data.PROBABLITY)
    plt.show() 