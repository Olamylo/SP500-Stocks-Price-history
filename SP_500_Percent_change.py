import yfinance as yf
import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
# from datetime import datetime
import datetime
from datetime import timedelta
import time
from sqlalchemy import create_engine
import requests
from bs4 import BeautifulSoup

engine = create_engine('mssql+pyodbc://Server_Name/Database_Name?driver=SQL+Server+Native+Client+11.0')

def get_date(df, bulkid, date_added):
    df.insert(0, 'bulk_id', bulkid)
    df.insert(1, 'date_added', date_added)
    return df

# symbols = ['IBM','MSFT','META','INTC','NEM','AU','AEM','GFI','AAPL','TSLA','PG','ZS','ADSK','QLYS']

# Make a GET request to the Wikipedia page with the list of S&P 500 stocks
url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
response = requests.get(url)

# Parse the HTML content
soup = BeautifulSoup(response.content, 'html.parser')

# Find the table with the list of stocks
table = soup.find('table', {'class': 'wikitable sortable'})

# Get the rows of the table
rows = table.find_all('tr')

# Initialize an empty list to store the symbols
symbols = []

# Iterate over the rows
for row in rows:
    # Get the cells of the row
    cells = row.find_all('td')
    # If the row has cells
    if cells:
        # Get the first cell, which contains the symbol
        symbol = cells[0].text
        # Add the symbol to the list
        symbols.append(symbol)

modified_list = [x.replace('123', '') for x in symbols]

for i in range(len(symbols)):
    # Remove the string '123' from each element
    symbols[i] = symbols[i].replace('\n', '')

# Print the list of symbols
print(symbols)

# get last data dump date
with engine.begin() as con:
    sql_command1 = f"""SELECT MAX(Date) as max_date FROM STAGING_SP500_STOCK_PERCENT_CHANGE;"""

    max_date = pd.read_sql(sql_command1, con)
    max_date = max_date.iloc[0, 0]
    max_date = max_date.strftime('%Y-%m-%d')
    # print(max_date)

# convert max_date date to datetime format
maxdate = datetime.datetime.strptime(max_date, '%Y-%m-%d')
# print(maxdate)

# get today's date
today = datetime.datetime.now().strftime('%Y-%m-%d')

# convert today's date to datetime format
today = datetime.datetime.now().strptime(today,'%Y-%m-%d')
# print(today)

# get next day date
next_day = maxdate

yesterday = today - datetime.timedelta(days=1)
# print(yesterday)

if yesterday > maxdate:
    print('True')
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    print(today)
    next_day = datetime.datetime.strftime(next_day,'%Y-%m-%d')
    print(next_day)
    yesterday = datetime.datetime.strftime(yesterday, '%Y-%m-%d')
    print(yesterday)


    data = yf.download(symbols, start= f'{next_day}', end=f'{yesterday}')
    # data = yf.download(symbols, start= "2020-01-01", end="2022-11-30")
    # print(data)

    # data = data['Adj Close']
    # data.reset_index(level=0, inplace=True)

    portfolio_returns = data['Adj Close'].pct_change()
    print(portfolio_returns)
    portfolio_returns.reset_index(level=0, inplace=True)
    # portfolio_returns.drop(index=0)

    # portfolio_returns.tail(portfolio_returns.shape[0] - 1)
    portfolio_returns = portfolio_returns.iloc[1:]
    print(portfolio_returns)
    # portfolio_returns = portfolio_returns.reset_index(drop=True)


    bulkid = time.strftime('%Y%m%d%H%M')
    date_added = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    df = get_date(portfolio_returns, bulkid, date_added)

    df.to_csv('./stock%_test.csv', index=False, encoding="utf-8")

    df.to_sql('STAGING_SP500_STOCK_PERCENT_CHANGE', con=engine, if_exists='append', index=False)
    # df.to_sql('STAGING_SP500_STOCK_PRICES_TEST', con=engine, if_exists='replace', index=False)
    # df.to_sql('STAGING_SP500_STOCK_PRICES_TEST', con=engine, if_exists='append', index=False)
else:
    print('The database already has the latest data')




