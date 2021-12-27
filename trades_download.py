import argparse
import os

from datetime import timezone, datetime

from timescaledb_util import TimeScaleDBUtil
from trades_download_util import TradesDownloadUtil

def main():
    # PostgreSQL設定
    _pg_config = {
        'user': os.environ['POSTGRES_USER'],
        'password': os.environ['POSTGRES_PASSWORD'],
        'host': os.environ['POSTGRES_HOST'],
        'port': os.environ['POSTGRES_PORT'],
        'database': os.environ['POSTGRES_DATABASE']
    }
    
    _dbutil = TimeScaleDBUtil(user = _pg_config['user'], password = _pg_config['password'], host = _pg_config['host'], port = _pg_config['port'], database = _pg_config['database'])
    _tradesutil = TradesDownloadUtil(_dbutil)
    _exchange_list = list(_tradesutil.trades_params.keys())
    
    # Commandline arguments
    parser = argparse.ArgumentParser(description='Download public trades from some Crypto CEX')

    parser.add_argument('exchange', help=f'exchange name. {_exchange_list}')
    parser.add_argument('symbol', help='symbol name. Example: BTC/USD')

    args = parser.parse_args()
    
    if args.exchange not in _exchange_list:
        print(f'{args.exchange} is not supported')
        return

    _tradesutil.download_trades(exchange=args.exchange, symbol=args.symbol, since_datetime=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc))

if __name__ == "__main__":
    main()