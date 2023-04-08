import pandas as pd
import ccxt
import time
import os
import datetime

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行


def save_spot_candle_data_from_exchange(exchange, instId, time_interval, start_time, end_time, path):
    # ===开始抓取数据
    df_list = []
    end_time_since = exchange.parse8601(end_time)   #

    while True:
        params = {
            'instId': instId,
            'limit': '60',
            'bar': time_interval,
            'after': end_time_since # 从以前的时间开始往现在推
        }
        df = exchange.publicGetMarketCandles(params=params)

        # print(df['data'])

        # 整理数据
        df = pd.DataFrame(df['data'], dtype=float)  # 将数据转换为dataframe
        # 合并数据
        df_list.append(df)
        # 新的since
        t = pd.to_datetime(df.iloc[-1][0], unit='ms')
        end_time_since = exchange.parse8601(str(t))
        # 判断是否挑出循环
        if t < start_time or df.shape[0] <= 1:
            break
        # 抓取间隔需要暂停2s，防止抓取过于频繁
        time.sleep(1)

    # ===合并整理数据
    df = pd.concat(df_list, ignore_index=True)
    df.rename(columns={0: 'MTS', 1: 'open', 2: 'high',
                       3: 'low', 4: 'close', 5: 'volume'}, inplace=True)  # 重命名
    df['candle_begin_time'] = pd.to_datetime(df['MTS'], unit='ms')  # 整理时间
    df = df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']]  # 整理列的顺序

    # 选取数据时间段
    df = df[df['candle_begin_time'].dt.date == pd.to_datetime(start_time).date()]
    # 去重、排序
    df.drop_duplicates(subset=['candle_begin_time'], keep='last', inplace=True)
    df.sort_values('candle_begin_time', inplace=True)
    df.reset_index(drop=True, inplace=True)

    # ===保存数据到文件
    # 创建交易所文件夹
    path = os.path.join(path, exchange.id)
    if os.path.exists(path) is False:
        os.mkdir(path)
    # 创建spot文件夹
    path = os.path.join(path, '合约数据')
    if os.path.exists(path) is False:
        os.mkdir(path)
    # 创建日期文件夹
    path = os.path.join(path, str(pd.to_datetime(start_time).date()))
    if os.path.exists(path) is False:
        os.mkdir(path)
    # 拼接文件目录
    file_name = '_'.join([symbol.replace('/', '-'), time_interval]) + '.csv'
    path = os.path.join(path, file_name)
    # 保存数据
    df.to_csv(path, index=False)

# *************************下面这个路径path需要替换成自己电脑的地址，或者保存到当前文件夹
path = r'./'  # r'./'的含义就是当前文件夹
error_list = []

exchange = ccxt.okex5()

while True:
    now = datetime.datetime.utcnow()    # 获取当前时间
    today = datetime.datetime(now.year, now.month, now.day, 0, 0, 0) # 获取今天的日期
    yesterday = today - datetime.timedelta(days=1) # 往前推一天，获取昨天的日期
    # ****************下面这个list是需要调整的部分，如果当周合约到期，下一个交易日当周合约和当季合约会发生变化********************
    for symbol in ['BTC-USD-SWAP', 'BTC-USDT-SWAP',
                   'BTC-USDT-220930',
                   'BTC-USD-220930']:
        for time_interval in ['5m', '1H']:

            print(exchange.id, yesterday, symbol, time_interval)
            save_spot_candle_data_from_exchange(exchange, symbol, time_interval, yesterday, str(today), path)

            # try:
            #     save_spot_candle_data_from_exchange(exchange, symbol, time_interval, yesterday, str(today), path)
            # except Exception as e:
            #     print(e)
            #     error_list.append('_'.join([exchange.id, symbol, time_interval]))

    print(error_list)
    exit()

    # 下面的代码不要求掌握，作用sleep到下一个交易日再获取第二天的数据。
    # tomorrow = today + datetime.timedelta(days=1)
    # sleep_time = (tomorrow - datetime.datetime.utcnow()).seconds
    #
    # if sleep_time > 0:
    #     time.sleep(sleep_time)
