import requests
import pandas as pd
from bs4 import BeautifulSoup

# Replace with your Alpha Vantage API key
api_key = ''


# Function to get stock price by ISIN
def get_stock_price(isin):
    base_url = 'https://www.alphavantage.co/query'
    function = 'TIME_SERIES_INTRADAY'
    symbol = isin  # You can use ISIN as a symbol
    interval = '5min'  # You can change the interval as needed

    params = {
        'function': function,
        'symbol': symbol,
        'interval': interval,
        'apikey': api_key,
    }

    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        data = response.json()
        if 'Time Series (5min)' in data:
            latest_data = list(data['Time Series (5min)'].values())[0]
            stock_price = latest_data['4. close']
            return float(stock_price)
        else:
            return None
    else:
        print(f"Error: Unable to fetch data for {symbol}")
        return None


# Function to get company details by ISIN
def get_company_details(isin):
    base_url = 'https://www.alphavantage.co/query'
    function = 'OVERVIEW'
    symbol = isin  # You can use ISIN as a symbol

    params = {
        'function': function,
        'symbol': symbol,
        'apikey': api_key,
    }

    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        data = response.json()
        print(data)
        return data
    else:
        print(f"Error: Unable to fetch company details for {symbol}")
        return None


if __name__ == "__main__":

    final_df = pd.DataFrame()

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

    for symbol in symbols:
        print(symbol)
        stock_price = get_stock_price(symbol)
        company_details = get_company_details(symbol)

        if stock_price is not None:
            print(f"Stock Price: {stock_price}")

        if company_details is not None:
            print("Company Details:")
            # for key, value in company_details.items():
            #     print(f"{key}: {value}")

            df = pd.DataFrame.from_dict([company_details])
            # print(df)
            # df.to_csv('test.csv')
            final_df = pd.concat([final_df, df])

    final_df.to_csv('final_data_Sheet.csv')