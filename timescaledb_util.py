import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class TimeScaleDBUtil:
    def __init__(self, user = None, password = None, host = None, port = None, database = None):
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._database = database
        
        self._sqlalchemy_config = f'postgresql+psycopg2://{self._user}:{self._password}@{self._host}:{self._port}/{self._database}'
        self._engine = create_engine(self._sqlalchemy_config)
        
        # enum_side型の存在を確認し、なければ作る
        _df = self.read_sql_query("SELECT * from pg_type WHERE typname='enum_side'")
        if _df.empty == True:
            self.sql_execute("CREATE TYPE enum_side AS ENUM ('buy', 'sell')")

    def read_sql_query(self, sql = '', index_column = ''):
        df = pd.read_sql_query(sql, self._engine)
        if len(index_column) > 0:
            df = df.set_index(index_column)
        return df
    
    def sql_execute(self, sql = ''):
        return self._engine.execute(sql)
    
    def df_to_sql(self, df = None, schema = None, if_exists = 'fail'):
        if df.empty or schema == None:
            return  
        return df.to_sql(schema, con = self._engine, if_exists = if_exists, index = False)
    
    ### 約定履歴テーブル関係の処理
    def get_trade_table_name(self, exchange, symbol):
        return (f'{exchange}_{symbol}_trade').lower()
    
    def init_trade_table(self, exchange='binance', symbol='BTC/USDT', force=False):    
        _table_name = self.get_trade_table_name(exchange, symbol)
        
        _df = self.read_sql_query(f"select * from information_schema.tables where table_name='{_table_name}'")
        if _df.empty == False and force == False:
            return
        
        # トレード記録テーブルを作成
        _sql = (f'DROP TABLE IF EXISTS "{_table_name}" CASCADE;'
                f' CREATE TABLE IF NOT EXISTS "{_table_name}" (datetime TIMESTAMP WITH TIME ZONE NOT NULL, id text, side enum_side NOT NULL, price NUMERIC NOT NULL, amount NUMERIC NOT NULL, UNIQUE(datetime, id));'
                f' CREATE INDEX ON "{_table_name}" (datetime DESC);'
                f" SELECT create_hypertable ('{_table_name}', 'datetime');")
        self.sql_execute(_sql)
        
    def get_latest_trade(self, exchange='binance', symbol='BTC/USDT'):
        _table_name = self.get_trade_table_name(exchange, symbol)
        
        _df = self.read_sql_query(f'SELECT * FROM "{_table_name}" ORDER BY datetime DESC, id DESC LIMIT 1')
        if len(_df) > 0:
            return _df.iloc[0]
        
        return None
    
    ### 時間足テーブル関係の処理
    def get_time_table_name(self, exchange, symbol, interval):
        return (f'{exchange}_{symbol}_{interval}').lower()

    def init_time_aggregate_table(self, exchange='binance', symbol='BTC/USDT', interval='1 day', force = False):
        _trades_table_name = self.get_trade_table_name(exchange, symbol)
        _period_table_name = self.get_time_table_name(exchange, symbol, interval.replace(' ', ''))
        
        if force == True:
            sql = (f'DROP MATERIALIZED VIEW IF EXISTS "{_period_table_name}"')
            self.sql_execute(sql, debug = debug)
        else:
            _df = self.read_sql_query(f"SELECT * FROM pg_views WHERE viewname='{_period_table_name}';")
            if len(_df) > 0:
                return

        sql = (f'CREATE MATERIALIZED VIEW "{_period_table_name}" WITH (timescaledb.continuous) AS SELECT '
               f"time_bucket(INTERVAL '{interval}', datetime) AS time,"
               f"first(price, datetime) AS open,"
               f"MAX(price) AS high,"
               f"MIN(price) AS low,"
               f"last(price, datetime) AS close,"
               f"SUM(amount*price) AS volume,"
               f"SUM(CASE WHEN side='buy' THEN amount*price ELSE 0 END) AS buy_volume,"
               f"SUM(CASE WHEN side='sell' THEN amount*price ELSE 0 END) AS sell_volume,"
               f"COUNT(id) AS trades_count "
               f'FROM "{_trades_table_name}" GROUP BY time WITH NO DATA;'
               f"SELECT add_continuous_aggregate_policy('{_period_table_name}',start_offset => NULL,end_offset => '{interval}',schedule_interval => INTERVAL '{interval}');"
               f'ALTER MATERIALIZED VIEW "{_period_table_name}" set (timescaledb.materialized_only = false);')
        self.sql_execute(sql)
