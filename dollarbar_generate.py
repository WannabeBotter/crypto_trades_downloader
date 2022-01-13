import time
from tqdm import tqdm
import traceback

from datetime import timezone, datetime, timedelta
import dateutil.parser as dp
from decimal import Decimal
from math import ceil, floor
import pandas as pd

from timescaledb_util import TimeScaleDBUtil

def generate_dollarbar(target_exchange, target_symbol, interval):    
    # トレードとドルバーを入れる空のデータフレームを作る
    _df_trades = pd.DataFrame(columns=['datetime', 'id', 'side', 'price', 'amount', 'dollar', 'dollar_cumsum'])
    _df_dollarbars = pd.DataFrame(columns=['datetime', 'datetime_from', 'id', 'id_from', 'open', 'high', 'low', 'close', 'amount', 'dollar_volume', 'dollar_buy_volume', 'dollar_sell_volume', 'dollar_cumsum'])
    
    # 計算済みの最新のドルバーを取得する
    _latest_dollarbar = trades_utils.get_latest_dollarbar(target_exchange, target_symbol, interval)
    if _latest_dollarbar is None:
        print('There is no dollar bar calculated. Start from the beginning of downloaded trade data.')
    else:
        print(f'The latest dollar bar is as follows. Resume from the end of the dollar bar.')
        print(_latest_dollarbar)
    
    # 読み飛ばす最後の約定データのtrade_id
    _target_trade_id = -1
    
    if _latest_dollarbar is not None:
        _temp_datetime = _latest_dollarbar['time']
        _target_trade_filename = f'{_trades_directory}/{_temp_datetime.year:04d}-{_temp_datetime.month:02d}-{_temp_datetime.day:02d}.pkl.gz'
        _target_trade_id = _latest_dollarbar['trade_id_tail']

        # 読み飛ばしてよい約定データファイルをリストから消す
        for _pkl_file in list(_trade_pkl_files):
            if _pkl_file == _target_trade_filename:
                break
            else:
                _trade_pkl_files.remove(_pkl_file)
    
    # 一本もドルバーを生成せずに次の日の約定履歴を読みだす場合に、処理中の日付を維持するためのフラグ
    # 初期状態では処理中の日付を更新したいのでFalseにしておく
    _keep_processing_datetime = False
    
    for _pkl_file in tqdm(_trade_pkl_files):
        # 一日分の約定情報を読み込み、すでに読み込み済みの約定情報データフレームに結合
        _df_newday_trades = pd.read_pickle(_pkl_file)
        _df_trades = pd.concat([_df_trades, _df_newday_trades])
        _df_trades['dollarbar_id'] = _df_trades['cumsum_total'] // dollarbar_unit
        
        if _target_trade_id >= 0 and _df_trades['id'].max() < _target_trade_id:
            continue
        else:
            _df_trades = _df_trades.loc[_df_trades['id'] > _target_trade_id]
            _target_trades_id = -1
                
        if _keep_processing_datetime == False:
            _processing_datetime = _df_trades.iloc[0]['time']
        
        # 現在処理中の日の最後のドルバーIDを取得
        _temp_datetime = _processing_datetime + timedelta(days=1)
        _temp_datetime = datetime(_temp_datetime.year, _temp_datetime.month, _temp_datetime.day, 0, 0, 0, tzinfo=timezone.utc)
        _last_dollarbar_id = _df_trades.loc[_df_trades['time'] < _temp_datetime].iloc[-1]['dollarbar_id']
        
        # 現在読み込み済みの約定情報の末尾のドルバーIDが現在処理中の最後のドルバーIDと同じ場合、次の日の約定情報を読み込まなければならない
        if _df_trades.iloc[-1]['dollarbar_id'] == _last_dollarbar_id:
            _keep_processing_datetime = True
            continue
        
        # ドルバー確定に十分な約定情報があるので、翌日に食い込んだ分も考慮してドルバー作成対象となる約定情報を抽出する
        _df_trades_new_dollarbars = _df_trades.loc[_df_trades['dollarbar_id'] <= _last_dollarbar_id]
            
        # グループを利用してドルバーを作成する
        _group_new_dollarbars = _df_trades_new_dollarbars.groupby('dollarbar_id', as_index=False)
        _df_aggregate = _group_new_dollarbars.apply(dollarbar_aggregate)
        _df_aggregate.drop('dollarbar_id', axis=1, inplace=True)
        
        # ドルバーのデータフレームに新しいドルバーを追加                
        _df_dollarbars = pd.concat([_df_dollarbars, _df_aggregate])
        
        # 対象日のドルバーができたので日付別のファイルに書き出す
        _df_dollarbars.reset_index(drop=True, inplace=True)
        _df_dollarbars.to_pickle(f'{_dollarbar_directory}/{_processing_datetime.year:04d}-{_processing_datetime.month:02d}-{_processing_datetime.day:02d}.pkl.gz', compression='gzip')
        
        # ドルバー用のデータフレームをクリア
        _df_dollarbars = pd.DataFrame(columns=trades_utils.DOLLARBAR_COLUMNS)
        
        # ドルバー生成に使った約定履歴を取り除く
        _df_trades = _df_trades.loc[_df_trades['dollarbar_id'] > _last_dollarbar_id]
        
        # 処理している日にちを先に進めるフラグを立てる
        _keep_processing_datetime = False