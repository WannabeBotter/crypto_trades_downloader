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
    
    # ダウンロード時に利用するパラメータの作成
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
    
    # 約定情報のダウンロード
    def download_trades(self, exchange=None, symbol=None, since_datetime=None):
        if exchange is None or symbol is None:
            return
        
        # 取引所情報の取得
        _exchange = exchange
        _ccxt_client = getattr(ccxt, _exchange)()
        _ccxt_client.load_markets()
        _ccxt_market = _ccxt_client.market(symbol)
        _price_precision = _ccxt_market['precision']['price']
        _amount_precision = _ccxt_market['precision']['amount']
        
        # 約定テーブルを初期化
        self._dbutil.init_trade_table(_exchange, symbol, force=False)
        _trade_table_name = self._dbutil.get_trade_table_name(_exchange, symbol)

        # デフォルトの開始時間と取引額オフセット
        _since_datetime = datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        _dollar_cumsum_offset = Decimal(0)

        # 約定データがDBにすでにあるならば最も新しい約定を取得して開始時間として設定
        _latest_trade = self._dbutil.get_latest_trade(_exchange, symbol)
        if _latest_trade is not None:
            _since_datetime = _latest_trade['datetime'] + timedelta(seconds=float(self.trades_params[exchange]['start_adjustment_timeunit']/1_000_000_000))
            _dollar_cumsum_offset = Decimal(_latest_trade['dollar_cumsum'])
            print('Dowload will resume after this last trade in DB')
            print(_latest_trade)
        else:
            _since_datetime = since_datetime

        _since_timestamp_nsec = Decimal(_since_datetime.timestamp()).quantize(Decimal('0.000001')) * 1_000_000_000
        _start_timestamp_nsec = _since_timestamp_nsec

        _till_datetime = datetime.now(timezone.utc)
        _till_timestamp_nsec = Decimal(_till_datetime.timestamp()).quantize(Decimal('0.000001')) * 1_000_000_000

        _total_seconds_nsec = _till_timestamp_nsec - _since_timestamp_nsec
        
        # 初期のダウンロード間隔は30分ごと
        if self.trades_params[exchange]['max_interval'] > 0:
            _interval_nsec = Decimal(30*60*1_000_000_000)
            _end_timestamp_nsec = _start_timestamp_nsec + _interval_nsec
        else:
            # 最大間隔が0以下の場合、間隔は利用せず、終了時間は最終終了時間を設定する
            _interval_nsec = Decimal(-1)
            _end_timestamp_nsec = _till_timestamp_nsec
        
        with tqdm(total = int(_total_seconds_nsec), initial=0) as _pbar:
            while _start_timestamp_nsec < _till_timestamp_nsec:
                try:
                    time.sleep(_ccxt_client.rateLimit * self.trades_params[exchange]['ratelimit_multiplier'] / 1000)
                    
                    # 取得最大間隔が0よりも大きい場合、今回のダウンロードの終了時間を更新する
                    if self.trades_params[exchange]['max_interval'] > 0:
                        _end_timestamp_nsec = _start_timestamp_nsec+_interval_nsec
                    
                    _params = self._get_fetch_trades_params(exchange, _start_timestamp_nsec, _end_timestamp_nsec)
                    _result = _ccxt_client.fetch_trades(symbol, params=_params)

                    if self.trades_params[exchange]['max_interval'] > 0 and len(_result) >= self.trades_params[exchange]['limit']:
                        # もし取得間隔を利用するダウンロードで、取得した約定の件数がAPIの返す個数の上限値と同じか大きかったら、取得間隔を短くして再取得する
                        _interval_nsec = max(Decimal(1), floor(_interval_nsec*Decimal(0.5)))
                        _interval_nsec = int(_interval_nsec // self.trades_params[exchange]['start_adjustment_timeunit']) * self.trades_params[exchange]['start_adjustment_timeunit']

                        # プログレスバーを更新
                        _pbar.set_postfix_str(f'{_exchange}, {symbol}, start: {datetime.utcfromtimestamp(float(_start_timestamp_nsec/1_000_000_000))}, interval: {_interval_nsec/1_000_000_000:.03f}, row_counts: {len(_result)}')
                        _pbar.refresh()
                        continue

                    if len(_result) > 0:
                        # _resultにliquidationの情報を付加する
                        for _item in _result:
                            if 'liquidation' in _item['info']:
                                _item['liquidation'] = _item['info']['liquidation']
                            else:
                                _item['liquidation'] = False

                        # もし1個以上のデータがダウンロードされていたら、データベースに書き込む
                        _df = pd.DataFrame.from_dict(_result, dtype=str)
                        _df = _df[['datetime', 'id', 'side', 'liquidation', 'price', 'amount']].sort_values('datetime', ascending=True).sort_values('id', ascending=True)
                        _df.sort_values('datetime', ascending=True).sort_values('id', ascending=True)
                        _to_decimal = lambda x: Decimal(x)
                        _df['price'] = _df['price'].apply(_to_decimal)
                        _df['amount'] = _df['amount'].apply(_to_decimal)
                        _df['dollar'] = _df['price'] * _df['amount']
                        _df['dollar_cumsum'] = _df['dollar'].cumsum() + _dollar_cumsum_offset

                        self._dbutil.df_to_sql(df=_df, schema=_trade_table_name, if_exists = 'append')
                        
                        _dollar_cumsum_offset = _df.iloc[-1]['dollar_cumsum']
                        
                    # プログレスバーを更新
                    _pbar.set_postfix_str(f'{_exchange}, {symbol}, start: {datetime.utcfromtimestamp(float(_start_timestamp_nsec/1_000_000_000))}, interval: {_interval_nsec/1_000_000_000:.03f}, row_counts: {len(_result)}')
                    if self.trades_params[exchange]['max_interval'] > 0:
                        _pbar.n = int(_end_timestamp_nsec-_since_timestamp_nsec)
                    else:
                        if len(_result) > 0:                    
                            _pbar.n = int(Decimal(dp.parse(_df.iloc[-1]['datetime']).timestamp()).quantize(Decimal('0.000001'))*1_000_000_000-_since_timestamp_nsec)
                    _pbar.refresh()
                    
                    # 約定データの取得間隔を調整
                    if self.trades_params[exchange]['max_interval'] > 0:
                        if len(_result) < self.trades_params[exchange]['limit']*0.9:
                            _interval_nsec = min(self.trades_params[exchange]['max_interval'], ceil(_interval_nsec / self.trades_params[exchange]['start_adjustment_timeunit'] * Decimal(1.05)) * self.trades_params[exchange]['start_adjustment_timeunit'])
                            _interval_nsec = int(_interval_nsec // self.trades_params[exchange]['start_adjustment_timeunit']) * self.trades_params[exchange]['start_adjustment_timeunit']
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
                    break
