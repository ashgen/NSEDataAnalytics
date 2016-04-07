'''Python code for scenario analysis of Option with underlying stock change,change in volatility in time
'''
import vollib
import pandas as pd
import config
import datetime as dt
import MySQLdb
from gdata.contentforshopping.data import Price
from vollib.black_scholes.implied_volatility import implied_volatility
from vollib.black_scholes.greeks.analytical import delta,vega,theta,gamma 
from vollib.black_scholes import black_scholes
from itertools import product
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from matplotlib import cm
from matplotlib.ticker import LinearLocator, FormatStrFormatter
r=0.1

class Scenario(object):
    '''Scenario Class with changes in the Future , Volatility and time to expiry '''
    def __init__(self,fut,vol,expiry_delta):
        self.fut=fut
        self.vol=vol
        self.expiry_delta=expiry_delta

class PnL(object):
    def __init__(self,fut,vol,profit):
        self.fut=fut
        self.vol=vol
        self.profit=profit
         
            
class Option(object):
    global r
    def __init__(self,symbol,strike,type="CE",valdate=None,price=None,expiry=None):
        self.symbol=symbol
        self.valdate=(dt.datetime.today()) if valdate ==None else valdate
        self.type=type
        self.strike=strike
        self.price=price if price != None else None
        self.expiry=expiry if expiry !=None else None
        self.get_opt_data()
        self.calc_iv_greeks()
        
    def get_opt_data(self):
        db=MySQLdb.connect(config.host,config.user,config.password,'NSE')
        self.valdate=pd.read_sql("select timestamp from %s order by timestamp desc limit 1"%self.symbol,db).timestamp[0]
        sql='select settle_pr,expiry_dt from %s where timestamp=STR_TO_DATE("%s","%%Y-%%m-%%d") and STRIKE_PR=%f and OPTION_TYP="%s" and MONTH_CODE="1M"'
        data=pd.read_sql(sql%(self.symbol,self.valdate.date().__str__(),self.strike,self.type), con=db)
        fut_sql='select settle_pr from %s where timestamp=STR_TO_DATE("%s","%%Y-%%m-%%d") and MONTH_CODE="1M" and (INSTRUMENT="FUTSTK" or INSTRUMENT="FUTIDX")'
        fut=pd.read_sql(fut_sql%(self.symbol,self.valdate.date().__str__()),con=db)
        db.close()
        self.underlying=fut.settle_pr[0]
        self.price=data.settle_pr[0] 
        self.expiry=data.expiry_dt[0]
        self.timedelta=(self.expiry-self.valdate).total_seconds()/(365.0*24*60*60)
        self.flag='c' if self.type=="CE" else 'p'
    
    def calc_iv_greeks(self):
        '''(price, S, K, t, r, flag
        (flag, S, K, t, r, sigma)'''
        global r
        
        self.vol=implied_volatility(self.price,self.underlying,self.strike,self.timedelta,r,self.flag)
        self.delta=delta(self.flag,self.underlying,self.strike,self.timedelta,r,self.vol)
        self.gamma=gamma(self.flag,self.underlying,self.strike,self.timedelta,r,self.vol)
        self.vega=vega(self.flag,self.underlying,self.strike,self.timedelta,r,self.vol)
        self.theta=theta(self.flag,self.underlying,self.strike,self.timedelta,r,self.vol)
    
    
        
    
        
    def recalc(self,Scenario):
        global r
        S=self.underlying*(1+Scenario.fut/100.0)
        t=(self.expiry-(self.valdate+dt.timedelta(days=Scenario.expiry_delta))).total_seconds()/(365.0*24*60*60)
        sigma=self.vol+Scenario.vol/100.0
        self.scenario_price=black_scholes(self.flag, S, self.strike, t, r, sigma)
        self.PnL=100*(self.scenario_price-self.price)/self.price
        return PnL(S,sigma,self.PnL)
        


                
if __name__=='__main__':
    option=Option(symbol="AXISBANK",strike=430,type="CE")
    Scenarios=[Scenario(fut=f,vol=v,expiry_delta=7) for f,v in product(np.arange(-20,21,1),np.arange(-10,15,5))]
    profits=[option.recalc(s) for s in Scenarios]
    X=[pro.fut for pro in profits]
    Y=[pro.vol for pro in profits]
    Z=[pro.profit for pro in profits]
    fig = plt.figure()
    ax = fig.gca(projection='3d')
    X, Y = np.meshgrid(X, Y)
    ax.plot_surface(X, Y, Z, rstride=8, cstride=8, alpha=0.3)
    cset = ax.contour(X, Y, Z, zdir='z', offset=-100, cmap=cm.coolwarm)
    cset = ax.contour(X, Y, Z, zdir='x', offset=-40, cmap=cm.coolwarm)
    cset = ax.contour(X, Y, Z, zdir='y', offset=40, cmap=cm.coolwarm)
    ax.set_zlim(-100, 2000)
    
    ax.set_xlabel('Future')
    ax.set_ylabel('Volatility')
    ax.set_zlabel('Profits')
    

    fig.colorbar(surf, shrink=0.5, aspect=5)
    plt.show()
    
    
        
    
