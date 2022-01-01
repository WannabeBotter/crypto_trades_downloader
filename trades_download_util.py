import time
from tqdm import tqdm
import traceback

from datetime import timezone, datetime, timedelta
import dateutil.parser as dp
from decimal import Decimal
from math import ceil, floor
import pandas as pd

import ccxt

from timescaledb_util import TimeScaleDBUtil

class TradesDownloadUtil:
    trades_params = {
        'bequant': {
            'limit': 1000,
            'max_interval': Decimal(24*60*60*1_000_000_000),
            'start_adjustment_timeunit': Decimal(1_000_000),
            'start_adjustment': True,
            'ratelimit_multiplier': 1.2,
        },
        'binance': {
            'limit': 1000,
            'max_interval': Decimal(60*60*1_000_000_000)-Decimal(1_000_000_000),
            'start_adjustment_timeunit': Decimal(1_000_000),
            'start_adjustment': True,
            'ratelimit_multiplier': 1.0,
        },
        'bitfinex2': {
            'limit': 1000,
            'max_interval': Decimal(24*60*60*1_000_000_000),
            'start_adjustment_timeunit': Decimal(1_000_000),
            'start_adjustment': True,
            'ratelimit_multiplier': 1.2,
        },
        'ftx': {
            'limit': 5000,
            'max_interval': Decimal(24*60*60*1_000_000_000),
            'start_adjustment_timeunit': Decimal(1_000_000_000),
            'start_adjustment': False,
            'ratelimit_multiplier': 1.0
        },
        'kraken': {
            'limit': 1000,
            'max_interval': Decimal(-1),
            'start_adjustment_timeunit': Decimal(1_000),
            'start_adjustment': True,
            'ratelimit_multiplier': 1.0
        },
        'poloniex': {
            'limit': 1000,
            'max_interval': Decimal(24*60*60*1_000_000_000),
            'start_adjustment_timeunit': Decimal(1_000_000_000),
            'start_adjustment': True,
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
            params['start'] = int(start_timestamp/1_000_000)
            params['end'] = int(end_timestamp/1_000_000)
            params['limit'] = self.trades_params[exchange]['limit']
            params['sort'] = 1
        elif exchange == 'binance':
            params['startTime'] = int(start_timestamp/1_000_000)
            params['endTime'] = int(end_timestamp/1_000_000)
            params['limit'] = self.trades_params[exchange]['limit']
        elif exchange == 'ftx':
            params['start_time'] = int(start_timestamp/1_000_000_000)
            params['end_time'] = int(end_timestamp/1_000_000_000)
        elif exchange == 'kraken':
            params['since'] = int(start_timestamp)
        elif exchange == 'poloniex':
            params['start'] = int(start_timestamp/1_000_000_000)
            params['end'] = int(end_timestamp/1_000_000_000)
            params['limit'] = self.trades_params[exchange]['limit']
        elif exchange == 'bequant':
            datetime_from = datetime.fromtimestamp(float(start_timestamp/1_000_000_000), tz=timezone.utc)
            datetime_till = datetime.fromtimestamp(float(end_timestamp/1_000_000_000), tz=timezone.utc)
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

        # Initialize trade table
        self._dbutil.init_trade_table(_exchange, symbol, force=False)
        _trade_table_name = self._dbutil.get_trade_table_name(_exchange, symbol)

        # Default since datetime
        _since_datetime = datetime(2010, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        _latest_trade = self._dbutil.get_latest_trade(_exchange, symbol)
        if _latest_trade is not None:
            _since_datetime = _latest_trade['datetime'] + timedelta(seconds=float(self.trades_params[exchange]['start_adjustment_timeunit']/1_000_000_000))

        if since_datetime is not None and since_datetime > _since_datetime:
            _since_datetime = since_datetime

        _since_timestamp_nsec = Decimal(_since_datetime.timestamp()).quantize(Decimal('0.000001')) * 1_000_000_000 # In nanosec to support Kraken
        _start_timestamp_nsec = _since_timestamp_nsec

        _till_datetime = datetime.now(timezone.utc)
        _till_timestamp_nsec = Decimal(_till_datetime.timestamp()).quantize(Decimal('0.000001')) * 1_000_000_000

        _total_seconds_nsec = _till_timestamp_nsec - _since_timestamp_nsec
        print(_since_timestamp_nsec)
        
        # initial interval is 60 min in sec
        if self.trades_params[exchange]['max_interval'] > 0:
            _interval_nsec = Decimal(30*60*1_000_000_000)
            _end_timestamp_nsec = _start_timestamp_nsec + _interval_nsec
        else:
            _interval_nsec = Decimal(-1)
            _end_timestamp_nsec = _till_timestamp_nsec
        
        with tqdm(total = int(_total_seconds_nsec), initial=0) as _pbar:
            while _start_timestamp_nsec < _till_timestamp_nsec:
                try:
                    time.sleep(ccxt_client.rateLimit * self.trades_params[exchange]['ratelimit_multiplier'] / 1000)
                    
                    if self.trades_params[exchange]['max_interval'] > 0:
                        _end_timestamp_nsec = _start_timestamp_nsec+_interval_nsec
                    
                    _params = self._get_fetch_trades_params(exchange, _start_timestamp_nsec, _end_timestamp_nsec)
                    _result = ccxt_client.fetch_trades(symbol, params=_params)

                    # Too many results. Reduce _end_timestamp by half to reduce the result and retry
                    if self.trades_params[exchange]['max_interval'] > 0 and len(_result) >= self.trades_params[exchange]['limit']:
                        _interval_nsec = max(Decimal(1), floor(_interval_nsec*Decimal(0.5)))
                        _interval_nsec = int(_interval_nsec // self.trades_params[exchange]['start_adjustment_timeunit']) * self.trades_params[exchange]['start_adjustment_timeunit']
                    else:
                        if len(_result) > 0:
                            _df = pd.DataFrame.from_dict(_result)
                            _df = _df[['datetime', 'id', 'side', 'price', 'amount']].sort_values('datetime', ascending=True).sort_values('id', ascending=True)

                            self._dbutil.df_to_sql(df=_df, schema=_trade_table_name, if_exists = 'append')
                            
                            # Update progress bar only when DB write happens
                            if len(_result) > 0:
                                if self.trades_params[exchange]['max_interval'] > 0:
                                    _pbar.n = int(min(_till_timestamp_nsec-_since_timestamp_nsec, _end_timestamp_nsec-_since_timestamp_nsec))
                                else:
                                    _pbar.n = int(min(_till_timestamp_nsec-_since_timestamp_nsec, Decimal(dp.parse(_df.iloc[-1]['datetime']).timestamp()).quantize(Decimal('0.000001'))*1_000_000_000-_since_timestamp_nsec))

                                _pbar.set_postfix_str(f'{_exchange}, {symbol}, start: {datetime.utcfromtimestamp(float(_start_timestamp_nsec/1_000_000_000))}, interval: {_interval_nsec/1_000_000_000:.03f}, row_counts: {len(_result)}')
                            else:
                                _pbar.n = int(_start_timestamp_nsec - _since_timestamp_nsec)
                                _pbar.set_postfix_str(f'Exchange: {_exchange}, Symbol: {symbol}, Date = {datetime.utcfromtimestamp(float(_start_timestamp_nsec/1_000_000_000))}, results: 0')
                            _pbar.refresh()
                        
                        if self.trades_params[exchange]['max_interval'] > 0 and len(_result) < self.trades_params[exchange]['limit']*0.9:
                            _interval_nsec = min(self.trades_params[exchange]['max_interval'], ceil(_interval_nsec * Decimal(1.05)))
                            _interval_nsec = int(_interval_nsec // self.trades_params[exchange]['start_adjustment_timeunit']) * self.trades_params[exchange]['start_adjustment_timeunit']
                        if self.trades_params[exchange]['max_interval'] > 0:
                            if self.trades_params[exchange]['start_adjustment'] is True:
                                _start_timestamp_nsec = _end_timestamp_nsec + self.trades_params[exchange]['start_adjustment_timeunit']
                            else:
                                _start_timestamp_nsec = _end_timestamp_nsec
                        else:
                            if exchange == 'kraken':
                                _start_timestamp_nsec = Decimal(_df.iloc[-1]['id'])

                except ccxt.NetworkError as e:
                    print(f'ccxt.NetworkError : {e}')
                    pass
                except ccxt.ExchangeError as e:
                    print(f'ccxt.ExchangeError : {e}')
                    break
                except:
                    print(f'Other exceptions : {traceback.format_exc()}')
                    print(_params)
                    print(_result)
                    break
