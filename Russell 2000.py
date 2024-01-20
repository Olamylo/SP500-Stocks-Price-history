import yfinance as yf


def get_russell_2000_tickers():
    # Download the list of Russell 2000 constituents
    russell_2000 = yf.Ticker('^RUT')
    russell_2000_data = russell_2000.history(period="1d")
    russell_2000_constituents = russell_2000_data.index[0].to_pydatetime().strftime('%Y-%m-%d')

    # Download the list of constituents
    russell_2000_constituents = yf.download(russell_2000_constituents)

    # Extract and return the stock tickers (symbols)
    tickers = russell_2000_constituents.index.tolist()
    return tickers


if __name__ == "__main__":
    russell_2000_tickers = get_russell_2000_tickers()

    if russell_2000_tickers:
        print("Russell 2000 Constituent Tickers:")
        for ticker in russell_2000_tickers:
            print(ticker)
    else:
        print("Error: Unable to fetch Russell 2000 constituent tickers.")
