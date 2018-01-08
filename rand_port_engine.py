#Author_Trevor Lack

import pandas as pd
import pymysql
import numpy as np
from sqlalchemy import create_engine
from MySQL_Authorization import MySQL_Auth
from datetime import datetime
from CALENDAR import *
import sys
import time
import random
import multiprocessing as mp


#access_token = MySQL_Auth()
#conn = pymysql.connect(host='localhost', port=3306, user='tlack', passwd=access_token, db='bens_desk')
#engine = create_engine('mysql+pymysql://tlack:%s@localhost/bens_desk' %(access_token))
tic = time.clock()

class Portfolio:
    #corr_matrix = Corr_Matrix()
    universe_list = pd.read_csv('UNIVERSE_test.csv')
    INDEX_YTM = 6
    INDEX_SECTORS = universe_list[['GLIC', 'WEIGHT']].groupby(['GLIC']).sum()
    print(INDEX_SECTORS)

    def __init__(self):
        self.result = None
        self.MASTER_universe = self.universe_list
        self.draw_down_universe = self.MASTER_universe
        self.port_exposure = 0
        self.Curr_Portfolio = pd.DataFrame(columns=['CUSIP', 'WEIGHT', 'YTM', 'GLIC', 'port_weight'])
        self.TARGET_YTM = self.INDEX_YTM
        self.TARGET_SECTORS = self.INDEX_SECTORS

    '''Simultaneously select a cusip at random from the inventory and update remaining inventory'''
    def take_rand_cusip(self, inventory):
        drawn_short = self.draw_down_universe
        cusip_list = inventory['CUSIP'].tolist()
        random.shuffle(cusip_list)
        taken = cusip_list[0]
        # UNIVERSE minus the one cusip taken out
        self.draw_down_universe = drawn_short.loc[drawn_short['CUSIP'] != taken]
        # Add picked CUSIP and data to the current portfolio
        picked = inventory.loc[inventory['CUSIP'] == taken]
        #self.Curr_Portfolio = self.Curr_Portfolio.append(picked, ignore_index=True)
        self.Curr_Portfolio = pd.concat([self.Curr_Portfolio, picked]).reset_index(drop = True)


    '''Add the random cusip to the portfolio at a random weight within Y% of index target'''
    def add_taken_cusip(self, plus1_port):
        indx_wght = plus1_port['WEIGHT'].iloc[-1]
        indx_wght_upper = indx_wght + (indx_wght * .4)
        indx_wght_lower = indx_wght - (indx_wght * .4)
        port_weight = random.uniform(indx_wght_lower, indx_wght_upper)
        plus1_port['WEIGHT'].iloc[-1] = port_weight
        self.Curr_Portfolio = plus1_port

    '''Calculate Portfolio Metrics to determine universe filters'''
    def portfolio_metrics(self):
        port_iter = self.Curr_Portfolio
        self.port_exposure = port_iter['WEIGHT'].sum()
        port_iter['port_weight'] = (port_iter['WEIGHT'] / port_iter['WEIGHT'].sum())*100
        port_YTM = (port_iter['port_weight'] * port_iter['YTM']).sum()
        #Compare GLIC weights
        port_sector = port_iter[['GLIC', 'port_weight']].groupby(['GLIC']).sum()
        combine = pd.merge(self.INDEX_SECTORS, port_sector, left_index=True, right_index=True, how='left').fillna(0)
        diff = (combine['WEIGHT'] - combine['port_weight']).reset_index()
        glics4universe = diff.iloc[:, 0].loc[diff.iloc[:, 1] > 0]
        self.Curr_Portfolio = port_iter
        return port_YTM, glics4universe

    '''Apply Universe Filter for next round of security selection'''
    def universe_filter(self):
        univ_short = self.draw_down_universe
        port_YTM, glics4universe = self.portfolio_metrics()
        #print(glics4universe)
        if port_YTM >= self.TARGET_YTM:
            filtered_universe = univ_short.loc[univ_short['YTM'] <= self.TARGET_YTM]
        else:
            filtered_universe = univ_short.loc[univ_short['YTM'] >= self.TARGET_YTM]
        if filtered_universe.empty:
            print('Universe filter broke on YTM')
        #GLIC Filter
        try:
            filtered_universe = univ_short.loc[univ_short['GLIC'].isin(glics4universe)]
        except Exception as e:
            print('Universe filter broke on GLIC criteria')

        return filtered_universe

    '''MASTER Class CALLER'''
    def run_it(self):

        while self.port_exposure < 80:

            if self.Curr_Portfolio.empty:
                #Run First Iteration
                self.take_rand_cusip(self.MASTER_universe)
            else:
                bonds = self.universe_filter()
                self.take_rand_cusip(bonds)
            self.add_taken_cusip(self.Curr_Portfolio)

        self.Curr_Portfolio['port_weight'] = 100 * (self.Curr_Portfolio['WEIGHT'] /
                                                    self.Curr_Portfolio['WEIGHT'].sum())
        return self.Curr_Portfolio




for i in range(5000):
    file = 'C:\\Users\\tlack\Documents\\Python Scripts\\Python HYHG Analysis\\HYHG_Modeling\PICKLES2\\%s.pickle' % (i)
    a = Portfolio()
    sys.exit()
    #i = a.run_it()
    #i.to_pickle(file)
    del i
    toc = time.clock()
    print(tic - toc)

#a.to_csv('test1.csv')

#b = pd.read_pickle('test.pickle')
#print(b)


#print((a['YTM'] * a['port_weight']).sum())
#print(a[['GLIC', 'port_weight']].groupby(['GLIC']).sum())

sys.exit()
################

def mysql_dataset(dstart, dend):
    curr_date = dstart
    master_df = pd.DataFrame()

    while curr_date < dend:
        sql_loop = "SELECT hyhg_index.As_Of_Date, hyhg_index.CUSIP, hyhg_index.Price \
                        FROM hyhg_index WHERE As_Of_Date = \'%s\'"% (curr_date)
        df = pd.read_sql(sql_loop,conn)
        df['As_Of_Date'] = pd.to_datetime(df['As_Of_Date'])
        df = df.pivot(index='As_Of_Date', columns='CUSIP', values='Price')
        master_df = master_df.append(df)
        curr_string = curr_date.strftime('%Y-%m-%d')
        curr_date = get_next_day(curr_string)
        curr_date = datetime.strptime(curr_date, '%Y-%m-%d').date()
        master_df = master_df.dropna(how='any', axis=1)
    return master_df

def Corr_Matrix():
    dstart = dt.date(2017, 7, 3)
    dend = dt.date(2017, 8, 10)
    series = mysql_dataset(dstart,dend)
    series_pct = np.log(series / series.shift(1)).dropna(axis=0)
    series_cov = series_pct.cov()
    series_cov.to_csv('test2.csv')
    return series_cov


Matrix = Portfolio().rand_port(issues=10)
toc = time.clock()

print(tic-toc)
print(Matrix)

'''
Criteria for random portfolios:

min # of Cusips
min # of Issuers
sum of weights = 1
YTM proximity to benchmark
EFFDUR proximity to benchmark
EFFDV01 proximity to benchmark


'''