import pandas as pd
import os
import sys

os.chdir('R:/Common Files/Swaps Brokerage (Monthly)/Supporting Data/Python_Project_Support')

df = pd.read_csv('IVY_Broker_Data_Q3.csv')
df = df.rename(columns={'Commission (Base)':'Comm', 'Tran Trade Broker Description':'Broker', 'Account Number':'Fund'})
df = df[df.Comm >0]
print(df.tail())
print(df.columns.values)
print(df['Trade Date'].max())
print(df['Trade Date'].min())

print(df)

a = df.groupby(['Fund','Broker'])['Comm'].sum()
print(a)
a.to_csv('trevor.csv')