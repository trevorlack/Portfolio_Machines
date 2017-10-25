import pandas as pd
import pymysql
import numpy as np
from sqlalchemy import create_engine
from MySQL_Authorization import MySQL_Auth
import datetime
import sys

access_token = MySQL_Auth()
conn = pymysql.connect(host='localhost', port=3306, user='tlack', passwd=access_token, db='bens_desk')
engine = create_engine('mysql+pymysql://tlack:%s@localhost/bens_desk' %(access_token))


'''Create the Investable Universe to Construct Portfolios from'''
class Universe:
    def query(self, dater):
        hyhg_sql = "SELECT hyhg_liquidity.Date, hyhg_liquidity.ID_CUSIP\
         FROM hyhg_liquidity WHERE hyhg_liquidity.Date = \'%s\'" % (dater)

        return pd.DataFrame(pd.read_sql(hyhg_sql, conn))

class Portfollio:

    def __init__(self):
        self.population = population


    def rand_port(self, ids):
        C = self.population['ID_CUSIP'].sample(n=ids)

        return C

begin = datetime.date(2017, 5, 1)
#MASTER = Universe.query(begin)
#print(MASTER)
#B = Portfolio(MASTER).rand_port(30)

hyhg_sql = "SELECT hyhg_liquidity.Date, hyhg_liquidity.ID_CUSIP\
         FROM hyhg_liquidity WHERE hyhg_liquidity.Date = \'%s\'" % (begin)

tester = pd.DataFrame(pd.read_sql(hyhg_sql, conn))
test2 = tester['ID_CUSIP'].sample(n=15)
print(test2)

#print(B)


'''Create The Portfolios.  '''