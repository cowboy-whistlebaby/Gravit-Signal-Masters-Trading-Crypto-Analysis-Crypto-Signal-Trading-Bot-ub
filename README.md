[![Download Now](https://img.shields.io/badge/Download%20Here-Full%20version-purple)](https://telegra.ph/Download-05-02-264?opts931g23o03fc)

# Crypto Signal

The goal of this project is to provide an API that recommends Buy/Sell/Hold of Bitcoin at any given time to maximize profitability.


## Features

#### This release
- [x] Price prediction is here! Adapted Stocker class to predict Bitcoin price. Please see Stocker Prediction Usage.ipynb notebook found in this repository.
- [x] Export flat files from sqlitedb to use in Tableau.

#### Next release
- [ ] Incorporate OHLCV (open, high, low, close,volume) trends with ML algorithm.


    
![Orders as seen on cryptopia](https://github.com/upggr/ielko-microtrader/blob/master/screenshots/cryptopia.png)
   # Signaler service

Every minute, the signaler performs the following steps to make a prediction about whether the price is likely to increase or decrease:
* Retrieve the latest data from the server and update the current data window which includes some history (the history length is defined by a configuration parameter)
* Compute derived features based on the nearest history collected (which now includes the latest data). The features to be computed are described in the configuration file and are exactly the same as used in batch mode during model training
* Apply several (previously trained) ML models by forecasting some future values (not necessarily prices) which are also treated as (more complex) derived features. We apply several forecasting models (currently, Gradient Boosting, Neural network, and Linear regression) to several target variables (labels)
* Aggregate the results of forecasting produced by different ML models and compute the final signal score which reflects the strength of the upward or downward trend. Here we use many previously computed scores as inputs and derive one output score. Currently, it is implemented as an aggregation procedure but it could be based on a dedicated ML model trained on previously collected scores and the target variable. Positive score means growth and negative score means fall
* Use the final score for notifications
  ![Never loose a gainer again](https://github.com/upggr/ielko-microtrader/blob/master/screenshots/gainers.png)

Notes:
* The final result of the signaler is the score (between -1 and +1). The score should be used for further decisions about buying or selling by taking into account other parameters and data sources
* For the signaler service to work, trained models have to be available and stored in the "MODELS" folder. The models are trained in batch mode and the process is described in the corresponding section.
  howdtrader use sliqte database as default, and also support mongodb and
mysql if you want to use it. If you want to use other database, you can
change the configuration in howtrader/vt_setting.json, here is the full
configuration key, you can just config the corresponding values.
![tempsnip](https://user-images.githubusercontent.com/65098903/178709836-f52c5451-a3dc-410a-9404-ed0b931e587e.png)

```dict
{
    "font.family": "", # set font family if display error
    "font.size": 12,

    "log.active": True,
    "log.level": CRITICAL,
    "log.console": True,
    "log.file": True,

    "email.server": "smtp.qq.com",
    "email.port": 465,
    "email.username": "",
    "email.password": "",
    "email.sender": "",
    "email.receiver": "",

    "order_update_interval": 600, # securing correct orders' status by synchronizing/updating orders through rest api
    "update_server_time_interval": 300,  # sync with server time
    "passphrase": "howtrader",  # tv passphrase
    "port": 9999, # tv server port

    "datafeed.name": "",
    "datafeed.username": "",
    "datafeed.password": "",

    "database.timezone": get_localzone_name(),
    "database.name": "sqlite",
    "database.database": "database.db",
    "database.host": "",
    "database.port": 0,
    "database.user": "",
    "database.password": ""
}


```

to crawl the Binance exchange kline data for backtesting, you just need
to create a crawl_data.py file, just in the main.py directory. copy and
paste the following codes:

```
"""
use the binance api to crawl data then save into the sqlite database.

"""

import pandas as pd
import time
from datetime import datetime
import requests
import pytz
from howtrader.trader.database import get_database, BaseDatabase

pd.set_option('expand_frame_repr', False)  #
from howtrader.trader.object import BarData, Interval, Exchange

BINANCE_SPOT_LIMIT = 1000
BINANCE_FUTURE_LIMIT = 1500

from howtrader.trader.constant import LOCAL_TZ

from threading import Thread
database: BaseDatabase = get_database()

def generate_datetime(timestamp: float) -> datetime:
    """
    :param timestamp:
    :return:
    """
    dt = datetime.fromtimestamp(timestamp / 1000)
    dt = LOCAL_TZ.localize(dt)
    return dt


def get_binance_data(symbol: str, exchange: str, start_time: str, end_time: str):
    """
    crawl binance exchange data
    :param symbol: BTCUSDT.
    :param exchange: spot、usdt_future, inverse_future.
    :param start_time: format :2020-1-1 or 2020-01-01 year-month-day
    :param end_time: format: 2020-1-1 or 2020-01-01 year-month-day
    :param gate_way the gateway name for binance is:BINANCE_SPOT, BINANCE_USDT, BINANCE_INVERSE
    :return:
    """

    api_url = ''
    save_symbol = symbol
    gateway = "BINANCE_USDT"
    if exchange == 'spot':
        print("spot")
        limit = BINANCE_SPOT_LIMIT
        save_symbol = symbol.lower()
        gateway = 'BINANCE_SPOT'
        api_url = f'https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m&limit={limit}'

    elif exchange == 'usdt_future':
        print('usdt_future')
        limit = BINANCE_FUTURE_LIMIT
        gateway = "BINANCE_USDT"
        api_url = f'https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1m&limit={limit}'

    elif exchange == 'inverse_future':
        print("inverse_future")
        limit = BINANCE_FUTURE_LIMIT
        gateway = "BINANCE_INVERSE"
        f'https://dapi.binance.com/dapi/v1/klines?symbol={symbol}&interval=1m&limit={limit}'

    else:
        raise Exception('the exchange name should be one of ：spot, usdt_future, inverse_future')

    start_time = int(datetime.strptime(start_time, '%Y-%m-%d').timestamp() * 1000)
    end_time = int(datetime.strptime(end_time, '%Y-%m-%d').timestamp() * 1000)

    while True:
        try:
            print(start_time)
            url = f'{api_url}&startTime={start_time}'
            print(url)
            datas = requests.get(url=url, timeout=10, proxies=proxies).json()

            """
            [
                [
                    1591258320000,      // open time
                    "9640.7",           // open price
                    "9642.4",           // highest price
                    "9640.6",           // lowest price
                    "9642.0",           // close price(latest price if the kline is not close)
                    "206",              // volume
                    1591258379999,      // close time
                    "2.13660389",       // turnover
                    48,                 // trade count 
                    "119",              // buy volume
                    "1.23424865",       //  buy turnover
                    "0"                 // ignore
                ]

            """

            buf = []

            for row in datas:
                bar: BarData = BarData(
                    symbol=save_symbol,
                    exchange=Exchange.BINANCE,
                    datetime=generate_datetime(row[0]),
                    interval=Interval.MINUTE,
                    volume=float(row[5]),
                    turnover=float(row[7]),
                    open_price=float(row[1]),
                    high_price=float(row[2]),
                    low_price=float(row[3]),
                    close_price=float(row[4]),
                    gateway_name=gateway
                )
                buf.append(bar)

            database.save_bar_data(buf)

            # exit the loop, if close time is greater than the current time
            if (datas[-1][0] > end_time) or datas[-1][6] >= (int(time.time() * 1000) - 60 * 1000):
                break

            start_time = datas[-1][0]

        except Exception as error:
            print(error)
            time.sleep(10)


def download_spot(symbol):
    """
    download binance spot data, config your start date and end date(format: year-month-day)
    :return:
    """
    t1 = Thread(target=get_binance_data, args=(symbol, 'spot', "2018-1-1", "2018-6-1"))
    t2 = Thread(target=get_binance_data, args=(symbol, 'spot', "2018-6-1", "2018-12-1"))

    t3 = Thread(target=get_binance_data, args=(symbol, 'spot', "2018-12-1", "2019-6-1"))
    t4 = Thread(target=get_binance_data, args=(symbol, 'spot', "2019-6-1", "2019-12-1"))

    t5 = Thread(target=get_binance_data, args=(symbol, 'spot', "2019-12-1", "2020-6-1"))
    t6 = Thread(target=get_binance_data, args=(symbol, 'spot', "2020-6-1", "2020-12-1"))

    t7 = Thread(target=get_binance_data, args=(symbol, 'spot', "2020-12-1", "2021-6-1"))
    t8 = Thread(target=get_binance_data, args=(symbol, 'spot', "2021-6-1", "2021-12-1"))
    t9 = Thread(target=get_binance_data, args=(symbol, 'spot', "2021-12-1", "2022-6-28"))

    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t7.start()
    t8.start()
    t9.start()

    t1.join()
    t2.join()
    t3.join()
    t4.join()
    t5.join()
    t6.join()
    t7.join()
    t8.join()
    t9.join()


def download_future(symbol):
    """
    download binance future data, config your start date and end date(format: year-month-day)
    :return:

    """

    t1 = Thread(target=get_binance_data, args=(symbol, 'usdt_future', "2020-1-1", "2020-6-1"))
    t2 = Thread(target=get_binance_data, args=(symbol, 'usdt_future', "2020-6-1", "2020-12-1"))
    t3 = Thread(target=get_binance_data, args=(symbol, 'usdt_future', "2020-12-1", "2021-6-1"))
    t4 = Thread(target=get_binance_data, args=(symbol, 'usdt_future', "2021-6-1", "2021-12-1"))
    t5 = Thread(target=get_binance_data, args=(symbol, 'usdt_future', "2021-12-1", "2022-6-28"))

    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()

    t1.join()
    t2.join()
    t3.join()
    t4.join()
    t5.join()


# Disclaimer:
This project is for informational purposes only. You should not construe any such information or other material as legal, tax, investment, financial, or other advice. Nothing contained here constitutes a solicitation, recommendation, endorsement, or offer by me or any third party service provider to buy or sell any securities or other financial instruments in this or in any other jurisdiction in which such solicitation or offer would be unlawful under the securities laws of such jurisdiction.

If you plan to use real money, USE AT YOUR OWN RISK.

Under no circumstances will I be held responsible or liable in any way for any claims, damages, losses, expenses, costs, or liabilities whatsoever, including, without limitation, any direct or indirect damages for loss of profits.
If you plan to use real money, USE AT YOUR OWN RISK.

Under no circumstances will I be held responsible or liable in any way for any claims, damages, losses, expenses, costs, or liabilities whatsoever, including, without limitation, any direct or indirect damages for loss of profits.
