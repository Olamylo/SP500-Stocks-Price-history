import yfinance as yf
import pandas as pd


import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup

# engine = create_engine('mssql+pyodbc://Server_Name/Database_Name?driver=SQL+Server+Native+Client+11.0')

def get_date(df, bulkid, date_added):
    df.insert(0, 'bulk_id', bulkid)
    df.insert(1, 'date_added', date_added)
    return df


url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

response = requests.get(url)

soup = BeautifulSoup(response.content, 'html.parser')

table = soup.find('table', {'class': 'wikitable sortable'})

rows = table.find_all('tr')

symbols = []

for row in rows:
    cells = row.find_all('td')
    if cells:
        symbol = cells[0].text
        symbols.append(symbol)

for i in range(len(symbols)):
    # Remove the string '123' from each element
    symbols[i] = symbols[i].replace('\n', '')

print(symbols)

# symbols = ['AU','IBM','MSFT','META','INTC','NEM','AAPL','TSLA','PG','ZS','ADSK','QLYS']

final_df = pd.DataFrame()

for symbol in symbols:
    try:
        print(symbol)

        sym = yf.Ticker(symbol)

        # data = sym.get_financials()
        data = sym.get_institutional_holders()
        print(data)
        data['symbol'] = symbol

        final_df = pd.concat([final_df, data])

    except:
        print('error occured for ' + symbol + ' Moving on to next symbol')
        pass

final_df.to_csv('./stock_institutional_holders_SP500.csv', index=False, encoding="utf-8")





