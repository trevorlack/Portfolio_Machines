import pandas as pd
import pymysql
import numpy as np
from sqlalchemy import create_engine
from MySQL_Authorization import MySQL_Auth
from datetime import datetime
from CALENDAR import *
import sys

access_token = MySQL_Auth()
conn = pymysql.connect(host='localhost', port=3306, user='tlack', passwd=access_token, db='bens_desk')
engine = create_engine('mysql+pymysql://tlack:%s@localhost/bens_desk' %(access_token))


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

class Portfolio:
    corr_matrix = Corr_Matrix()

    def __init__(self):
        self.result = None

    def rand_port(self, issues):

        self.result = self.corr_matrix.sample(n=issues)
        return self.result


Matrix = Portfolio().rand_port(issues=10)

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