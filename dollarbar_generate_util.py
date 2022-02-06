import time
from tqdm import tqdm
import traceback
import os

from datetime import timezone, datetime, timedelta
import dateutil.parser as dp
from decimal import Decimal
from math import ceil, floor
import pandas as pd

from timescaledb_util import TimeScaleDBUtil
from trades_download_util import TradesDownloadUtil

def dollarbar_aggregate(x):
   y = {
       'datetime': x.iloc[-1]['datetime'],
       'datetime_from': x.iloc[0]['datetime'],
       'id': x.iloc[-1]['id'],
       'id_from': x.iloc[0]['id'],
       'open': x.iloc[0]['price'],
       'high': x['price'].max(),
       'low': x['price'].min(),
       'close': x.iloc[-1]['price'],
       'amount': x['amount'].sum(),
       'dollar_volume': x['dollar'].sum(),
       'dollar_buy_volume': x.loc[x['side'] == 'buy', 'dollar'].sum(),
       'dollar_sell_volume': x.loc[x['side'] == 'sell', 'dollar'].sum(),
       'dollar_liquidation_buy_volume': x.loc[(x['side'] == 'buy') & (x['liquidation'] == True), 'dollar'].sum(),
       'dollar_liquidation_sell_volume': x.loc[(x['side'] == 'sell') & (x['liquidation'] == True), 'dollar'].sum(),
       'dollar_cumsum': x.iloc[-1]['dollar_cumsum'],
       'buy_dollar_cumsum': x.iloc[-1]['buy_dollar_cumsum'],
       'sell_dollar_cumsum': x.iloc[-1]['sell_dollar_cumsum']
   }
   return pd.Series(y)

class DollarbarGenerateUtil:
    def __init__(self, dbutil=None):
        self._dbutil = dbutil
        self._tradesutil = TradesDownloadUtil(self._dbutil)
        self._exchange_list = list(self._tradesutil.trades_params.keys())

    def generate_dollarbar(self, exchange=None, symbol=None, interval=None):
        _exchange_list = list(self._tradesutil.trades_params.keys())
        
        if exchange not in _exchange_list:
            print(f'{exchange} is not supported')
            return
        
        # 約定情報をダウンロードする
        self._tradesutil.download_trades(exchange=exchange, symbol=symbol, since_datetime=datetime(2019, 3, 5, 0, 0, 0, tzinfo=timezone.utc))
        
        # トレードとドルバーを入れる空のデータフレームを作る
        _df_trades = pd.DataFrame(columns=['datetime', 'id', 'side', 'price', 'amount', 'dollar', 'dollar_cumsum'])
        _df_dollarbars = pd.DataFrame(columns=['datetime', 'datetime_from', 'id', 'id_from', 'open', 'high', 'low', 'close', 'amount', 'dollar_volume', 'dollar_buy_volume', 'dollar_sell_volume', 'dollar_liquidation_buy_volume', 'dollar_liquidation_sell_volume', 'dollar_cumsum'])
    
        # これよりも大きなdollar_cumsumを持つ約定データだけ読み込む
        _head_dollar_cumsum = 0

        # これよりも小さいか等しいdollar_cumsumを持つ約定データだけ読み込む
        _tail_dollar_cumsum = 0

        # 最新の約定情報を取得する
        _latest_trade = self._dbutil.get_latest_trade(exchange, symbol)
        if _latest_trade is None:
            print('There is no trade downloaded. Cannot calculate dollar bars')
            return
        else:
            _tail_dollar_cumsum = _latest_trade['dollar_cumsum']

        # 最古の約定情報を取得する
        _first_trade = self._dbutil.get_first_trade(exchange, symbol)
        if _first_trade is None:
            print('There is no trade downloaded. Cannot calculate dollar bars')
            return
        else:
            _head_dollar_cumsum = _first_trade['dollar_cumsum']
            _head_id = _first_trade['id']
            _head_datetime = _first_trade['datetime']

        # 計算済みの最新のドルバーを取得する
        _latest_dollarbar = self._dbutil.get_latest_dollarbar(exchange, symbol, interval)
        if _latest_dollarbar is None:
            print('There is no dollar bar calculated. Start from the beginning of downloaded trade data.')
        else:
            print(f'The latest dollar bar is as follows. Resume from the end of the dollar bar.')
            print(_latest_dollarbar)
            _head_dollar_cumsum = _latest_dollarbar['dollar_cumsum']
            _head_id = _latest_dollarbar['id']
            _head_datetime = _latest_dollarbar['datetime']

        _total_cumsum = _tail_dollar_cumsum - _head_dollar_cumsum
        _current_dollar_cumsum = _head_dollar_cumsum
        _current_id = _head_id
        _current_datetime = _head_datetime
    
        with tqdm(total = float(_total_cumsum), initial=0) as _pbar:
            _trade_table_name = self._dbutil.get_trade_table_name(exchange, symbol)
            self._dbutil.init_dollarbar_table(exchange, symbol, interval)
            _dollarbar_table_name = self._dbutil.get_dollarbar_table_name(exchange, symbol, interval)

            while _head_dollar_cumsum < _tail_dollar_cumsum:
                _sql = f'WITH time_filtered AS (SELECT * FROM \"{_trade_table_name}\" WHERE datetime >= \'{_current_datetime}\' ORDER BY datetime ASC LIMIT 10000) SELECT * from time_filtered WHERE dollar_cumsum > {_current_dollar_cumsum} AND id != \'{_current_id}\' ORDER BY dollar_cumsum ASC'
                _df_new_trades = self._dbutil.read_sql_query(_sql, dtype={'price': str, 'amount': str, 'dollar': str, 'dollar_cumsum': str, 'buy_dollar_cumsum': str, 'sell_dollar_cumsum': str})

                # もう読み込むデータがなければ終了
                if len(_df_new_trades) <= 0:
                    break

                _to_decimal = lambda x: Decimal(x)
                _df_new_trades['price'] = _df_new_trades['price'].apply(_to_decimal)
                _df_new_trades['amount'] = _df_new_trades['amount'].apply(_to_decimal)
                _df_new_trades['dollar'] = _df_new_trades['dollar'].apply(_to_decimal)
                _df_new_trades['dollar_cumsum'] = _df_new_trades['dollar_cumsum'].apply(_to_decimal)
                _df_new_trades['buy_dollar_cumsum'] = _df_new_trades['buy_dollar_cumsum'].apply(_to_decimal)
                _df_new_trades['sell_dollar_cumsum'] = _df_new_trades['sell_dollar_cumsum'].apply(_to_decimal)

                # 処理中のデータフレームに結合させ、ドルバーIDを再計算
                _df_trades = pd.concat([_df_trades, _df_new_trades]).sort_values('dollar_cumsum')
                _df_trades['dollarbar_id'] = _df_trades['dollar_cumsum'] // interval

                # プログレスバーを更新
                _pbar.set_postfix_str(f"{exchange}, {symbol}, start: {_df_trades.iloc[0]['datetime']}, row_counts: {len(_df_trades)} current_dollar_cumsum {_current_dollar_cumsum}")
                _pbar.n = float(_df_trades.iloc[0]['dollar_cumsum'] - _head_dollar_cumsum)
                _pbar.refresh()

                # 現在の約定情報データフレームの最初と最後のドルバーIDが一致していたら、ドルバーを生成できないので次のデータフレームをDBから読み込む
                if _df_trades.iloc[0]['dollarbar_id'] == _df_trades.iloc[-1]['dollarbar_id']:
                    _current_dollar_cumsum = _df_trades.iloc[-1]['dollar_cumsum']
                    _current_id = _df_trades.iloc[-1]['id']
                    _current_datetime = _df_trades.iloc[-1]['datetime']
                    continue

                # ドルバー確定に十分な約定情報があるので、ドルバー作成用の約定情報を抽出する
                _df_trades_new_dollarbars = _df_trades.loc[_df_trades['dollarbar_id'] < _df_trades.iloc[-1]['dollarbar_id']]

                # グループを利用してドルバーを作成する
                _group_new_dollarbars = _df_trades_new_dollarbars.groupby('dollarbar_id', as_index=False)
                _df_aggregate = _group_new_dollarbars.apply(dollarbar_aggregate)
                _df_aggregate.drop('dollarbar_id', axis=1, inplace=True)

                self._dbutil.df_to_sql(df=_df_aggregate, schema=_dollarbar_table_name, if_exists = 'append')

                # ドルバー生成に使った約定履歴を取り除く
                _df_trades = _df_trades.loc[_df_trades['dollarbar_id'] >= _df_trades.iloc[-1]['dollarbar_id']]
                _current_dollar_cumsum = _df_trades.iloc[-1]['dollar_cumsum']
                _current_id = _df_trades.iloc[-1]['id']
                _current_datetime = _df_trades.iloc[-1]['datetime']


