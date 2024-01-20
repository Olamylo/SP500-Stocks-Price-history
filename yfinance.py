import yfinance as yf

# CREATE A TICKER INSTANCE PASSING TESLA AS THE TARGET COMPANY
tsla = yf.Ticker('TSLA')

# CALL THE MULTIPLE FUNCTIONS AVAILABLE AND STORE THEM IN VARIABLES.
actions = tsla.get_actions()
# analysis = tsla.get_analysis()
balance = tsla.get_balance_sheet()
calendar = tsla.get_calendar()
cf = tsla.get_cashflow()
info = tsla.get_info()
inst_holders = tsla.get_institutional_holders()
news = tsla.get_news()
recommendations = tsla.get_recommendations()
# sustainability = tsla.get_sustainability()

# PRINT THE RESULTS
print(actions)
print('*'*20)
# print(analysis)
# print('*'*20)
print(balance)
print('*'*20)
print(calendar)
print('*'*20)
print(cf)
print('*'*20)
print(info)
print('*'*20)
print(inst_holders)
print('*'*20)
print(news)
print('*'*20)
print(recommendations)
print('*'*20)
# print(sustainability)
# print('*'*20)