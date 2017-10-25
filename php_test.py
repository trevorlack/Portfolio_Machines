import os
import pandas as pd

os.chdir('C:/Users/tlack/Desktop/GARBAGE')

df = pd.read_csv('cov_test.csv')
print(df)
df = df.cov()
print(df)


'''
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import sys

try:
    db = pymysql.connect(
        host='69.140.227.184:3306',
        user='root',
        passwd='admin',
        db = 'algo_data'
    )

except Exception as e:
    print(e)
    sys.exit('cant connect')

cursor = db.cursor()
query = ("SHOW DATABASES")
cursor.execute(query)
for r in cursor:
    print(r)

'''