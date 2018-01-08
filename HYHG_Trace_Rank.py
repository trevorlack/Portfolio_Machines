import pandas as pd
import pymysql
import numpy as np
from sqlalchemy import create_engine
from MySQL_Authorization import MySQL_Auth
import datetime
import sys

access_token = MySQL_Auth()
conn = pymysql.connect(host='localhost', port=****, user='****', passwd=access_token, db='****')
engine = create_engine('mysql+pymysql://*****:%s@localhost/*******' %(access_token))

conn2 = pymysql.connect(host='localhost', port=****, user='******', passwd=access_token, db='*******')
engine2 = create_engine('mysql+pymysql://******:%s@localhost/******' %(access_token))

dstart = datetime.date(2017,6,1)
dend = datetime.date(2017,6,30)

hyhg_sql = "SELECT hyhg_liquidity.Date, hyhg_liquidity.ID_CUSIP, hyhg_liquidity.Volume, \
        hyhg_liquidity.TRACE_Trades, hyhg_liquidity.recovery_rate, IX.As_Of_Date, \
        IX.CUSIP, IX.Ticker, IX.Price, IX.MKV, IX.PAR, IX.rebal_action \
    FROM ( \
	SELECT hyhg_index.As_Of_Date, hyhg_index.CUSIP, hyhg_index.Ticker, \
	        hyhg_index.Price, hyhg_index.MKV, hyhg_index.PAR, hyhg_index.rebal_action \
    FROM hyhg_index \
    WHERE hyhg_index.As_Of_Date \
		BETWEEN \'%s\' and \'%s\') AS IX \
    JOIN hyhg_liquidity ON hyhg_liquidity.Index_CUSIP = IX.CUSIP AND hyhg_liquidity.Date = IX.As_Of_Date \
    WHERE hyhg_liquidity.Date \
	    BETWEEN \'%s\' and \'%s\'" % (dstart, dend, dstart, dend)

hyhg_index_raw = pd.DataFrame(pd.read_sql(hyhg_sql, conn))
hyhg_index_raw['tl_perf'] = hyhg_index_raw['PAR'] * hyhg_index_raw['Price'] / 100
hyhg_index_raw.to_csv('test2.csv')
cusip_link = pd.DataFrame(pd.read_sql("SELECT * FROM cusip_link", conn))
c_raw_temp = hyhg_index_raw[hyhg_index_raw.Date == dend]
c_raw_temp2 = hyhg_index_raw[hyhg_index_raw.Date == dstart]
c_raw_temp2.to_csv('test1.csv')
c_link = cusip_link.loc[cusip_link['CUSIP2_9'].isin(c_raw_temp['ID_CUSIP'])]
c_link = c_link.loc[c_link['CUSIP1_9'].isin(c_raw_temp2['ID_CUSIP'])]

'''
if c_link['CUSIP2_9'].count() > 0:
    print(c_link)
    old_id = c_link['CUSIP1_9'].values.tolist()
    new_id = c_link['CUSIP2_9'].values.tolist()
    hyhg_index_raw = hyhg_index_raw.replace(to_replace=old_id, value=new_id)
'''

'''Create a list of unique CUSIPS and Tickers and calc the sum of each bonds volume over period.
Then create a list using only the most liquid bond for each ticker.'''
best_ticker = hyhg_index_raw[['Volume', 'ID_CUSIP', 'Ticker']].groupby(['ID_CUSIP', 'Ticker']).sum().reset_index(drop=False)
print(best_ticker)

'''idxmax method returns the cusip with the highest volume per issue'''
Liquid_optimized = best_ticker.loc[best_ticker.groupby(['Ticker'])['Volume'].idxmax()]
#Liquid_optimized.to_csv('test1.csv')

'''calculate starting weights by doing a groupby on first day of raw index'''
issue_weights = hyhg_index_raw[hyhg_index_raw.As_Of_Date == dstart]
print(issue_weights.groupby('Ticker').size().count())

issue_weights = issue_weights[['Ticker', 'MKV']]
issue_weights['Total_Cap'] = issue_weights['MKV'].sum()
issue_weights['weight'] = issue_weights['MKV'] / issue_weights['Total_Cap']
issue_weights = issue_weights[['Ticker', 'weight']].groupby(['Ticker']).sum()
issue_weights = issue_weights.reset_index()

Liquid_optimized = Liquid_optimized.merge(issue_weights, how = 'left', on = 'Ticker', indicator = True)

raw_perf1 = hyhg_index_raw[(hyhg_index_raw.As_Of_Date == dstart)]
raw_perf1 = raw_perf1[['As_Of_Date','ID_CUSIP', 'Price']]

Liquid_optimized = Liquid_optimized.merge(raw_perf1, how = 'left', on = 'ID_CUSIP')
Liquid_optimized = Liquid_optimized.rename(columns={'Price':'Start_Price'})
Liquid_optimized.to_csv('tester.csv')
raw_perf2 = hyhg_index_raw[(hyhg_index_raw.As_Of_Date == dend)]
raw_perf2 = raw_perf2[['As_Of_Date','ID_CUSIP', 'Price']]

raw_perf = float(
            (hyhg_index_raw.loc[hyhg_index_raw['As_Of_Date'] == dend, ['tl_perf']].sum() \
           - hyhg_index_raw.loc[hyhg_index_raw['As_Of_Date'] == dstart, ['tl_perf']].sum()) \
           / hyhg_index_raw.loc[hyhg_index_raw['As_Of_Date'] == dstart, ['tl_perf']].sum()
                 )

Liquid_optimized = Liquid_optimized.merge(raw_perf2, how = 'left', on = 'ID_CUSIP')
Liquid_optimized = Liquid_optimized.rename(columns={'Price':'End_Price'})
Liquid_optimized['Perf'] = (Liquid_optimized['End_Price'] - Liquid_optimized['Start_Price']) \
                / Liquid_optimized['Start_Price'] * Liquid_optimized['weight']
print(Liquid_optimized.groupby('Ticker').size().count())

Liquid_perf = float(Liquid_optimized['Perf'].sum())

print('Raw Performance Long Index: \n'
       + str(raw_perf))
print('Trace Optimized Long Index: \n'
      + str(Liquid_perf))
print('Volume Optimized Index Performance vs Raw Index: ' + str((Liquid_perf - raw_perf)*10000))


