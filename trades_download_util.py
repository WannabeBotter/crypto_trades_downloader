import time
from tqdm import tqdm
import traceback

from datetime import timezone, datetime, timedelta
from decimal import Decimal
from math import ceil, floor
import pandas as pd

import ccxt

from timescaledb_util import TimeScaleDBUtil

class TradesDownloadUtil:
    trades_params = {
        'bequant': {
            'limit': 1000,
            'max_interval': 24*60*60,
            'start_adjustment': 0.001,
            'ratelimit_multiplier': 1.2,
        },
        'binance': {
            'limit': 1000,
            'max_interval': 60*60,
            'start_adjustment': 0.001,
            'ratelimit_multiplier': 1.0,
        },
        'bitfinex2': {
            'limit': 1000,
            'max_interval': 24*60*60,
            'start_adjustment': 0.001,
            'ratelimit_multiplier': 1.2,
        },
        'ftx': {
            'limit': 5000,
            'max_interval': 24*60*60,
            'start_adjustment': 1.0,
            'ratelimit_multiplier': 1.0
        },
        'poloniex': {
            'limit': 1000,
            'max_interval': 24*60*60,
            'start_adjustment': 1.0,
            'ratelimit_multiplier': 1.0
        }
    }
    
    def __init__(self, dbutil=None):
        self._dbutil = dbutil
    
    def _get_fetch_trades_params(self, exchange=None, start_timestamp=None, end_timestamp=None):
        params = {}
        
        if exchange is None or start_timestamp is None or end_timestamp is None:
            print(f'Invalid args exchange={exchange}, start_timestamp={start_timestamp}, end_timestamp={end_timestamp}')
            return params
        
        if exchange == 'bitfinex2':
            params['start'] = int(start_timestamp*1000)
            params['end'] = int(end_timestamp*1000)
            params['limit'] = self.trades_params[exchange]['limit']
            params['sort'] = 1
        elif exchange == 'binance':
            params['startTime'] = int(start_timestamp*1000)
            params['endTime'] = int(end_timestamp*1000)
            params['limit'] = self.trades_params[exchange]['limit']
        elif exchange == 'ftx':
            params['start_time'] = int(start_timestamp)
            params['end_time'] = int(end_timestamp)
        elif exchange == 'poloniex':
            params['start'] = int(start_timestamp)
            params['end'] = int(end_timestamp)
            params['limit'] = self.trades_params[exchange]['limit']
        elif exchange == 'bequant':
            datetime_from = datetime.fromtimestamp(start_timestamp, tz=timezone.utc)
            datetime_till = datetime.fromtimestamp(end_timestamp, tz=timezone.utc)
            params['from'] = datetime_from.strftime('%Y-%m-%d %H:%M:%S.%f%z')
            params['till'] = datetime_till.strftime('%Y-%m-%d %H:%M:%S.%f%z')
            params['limit'] = self.trades_params[exchange]['limit']
            params['sort'] = 'ASC'
        
        return params
    
    def download_trades(self, exchange=None, symbol=None, since_datetime=None):
        if exchange is None or symbol is None:
            return
        
        _exchange = exchange
        ccxt_client = getattr(ccxt, _exchange)()

        self._dbutil.init_trade_table(_exchange, symbol, force=False)
        _trade_table_name = self._dbutil.get_trade_table_name(_exchange, symbol)

        _since_datetime = datetime(2010, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        _latest_trade = self._dbutil.get_latest_trade(_exchange, symbol)
        if _latest_trade is not None:
            _since_datetime = _latest_trade['datetime'] + timedelta(seconds=self.trades_params[exchange]['start_adjustment'])

        if since_datetime is not None and since_datetime > _since_datetime:
            _since_datetime = since_datetime

        _since_timestamp = _since_datetime.timestamp()
        _start_timestamp = _since_timestamp

        _till_datetime = datetime.now(timezone.utc)
        _till_timestamp = _till_datetime.timestamp()

        _total_seconds = _till_timestamp - _since_timestamp
        
        # initial interval is 60 min in sec
        _interval_sec = 60*60

        with tqdm(total = _total_seconds, initial=0) as _pbar:
            while _start_timestamp < _till_timestamp:                
                try:
                    time.sleep(ccxt_client.rateLimit * self.trades_params[exchange]['ratelimit_multiplier'] / 1000)
                    
                    _end_timestamp = int(_start_timestamp+_interval_sec)
                    _params = self._get_fetch_trades_params(exchange, _start_timestamp, _end_timestamp)
                    _result = ccxt_client.fetch_trades(symbol, params=_params)

                    # Too many results. Adjust _end_timestamp by half
                    if len(_result) >= self.trades_params[exchange]['limit']:
                        # Just update _interval_sec. Don't update _start_timestamp
                        _interval_sec = max(1, floor(_interval_sec*0.5))
                    else:
                        if len(_result) > 0:
                            _df = pd.DataFrame.from_dict(_result)
                            _df = _df[['datetime', 'id', 'side', 'price', 'amount']]
                            self._dbutil.df_to_sql(df=_df, schema=_trade_table_name, if_exists = 'append')
                            
                            # Update progress bar only when DB write happens
                            if len(_result) > 0:
                                _pbar.n = min(_till_timestamp-_since_timestamp, _end_timestamp-_since_timestamp)
                                _pbar.set_postfix_str(f'{_exchange}, {symbol}, start: {datetime.utcfromtimestamp(_start_timestamp)}, interval: {_interval_sec}, row_counts: {len(_result)}')
                            else:
                                _pbar.n = _start_timestamp - _since_timestamp
                                _pbar.set_postfix_str(f'Exchange: {_exchange}, Symbol: {symbol}, Date = {datetime.utcfromtimestamp(_start_timestamp)}, results: 0')
                            _pbar.refresh()
                        
                        if len(_result) < self.trades_params[exchange]['limit']*0.9:
                            _interval_sec = min(self.trades_params[exchange]['max_interval'], ceil(_interval_sec * 1.05))                                
                        _start_timestamp = _end_timestamp + self.trades_params[exchange]['start_adjustment'] # endTime in milliseconds is INCLUSIVE

                except ccxt.NetworkError as e:
                    print(f'ccxt.NetworkError : {e}')
                    pass
                except ccxt.ExchangeError as e:
                    print(f'ccxt.ExchangeError : {e}')
                    break
                except:
                    print(f'Other exceptions : {traceback.format_exc()}')
                    print(f'length of _result = {len(_result)}')
                    break