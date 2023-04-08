import ccxt

exchange = ccxt.binance()
symbol = 'BTC/USDT'
timeframe = '1m'
ohlcv = exchange.fetch_ohlcv(symbol, timeframe)
quote_volumes = [x[5] for x in ohlcv]
print(quote_volumes)

for x in ohlcv:
    print(x[5])