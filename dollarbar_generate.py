import os
import argparse

from timescaledb_util import TimeScaleDBUtil
from trades_download_util import TradesDownloadUtil
from dollarbar_generate_util import DollarbarGenerateUtil

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
    _dollarbarutil = DollarbarGenerateUtil(_dbutil)

    # Commandline arguments
    parser = argparse.ArgumentParser(description='Generate dollarbar from the date in TimescaleDB')

    parser.add_argument('exchange', help=f'exchange name. {_dollarbarutil._exchange_list}')
    parser.add_argument('symbol', help='symbol name. Example: BTC/USD')
    parser.add_argument('interval', help='Bar unit in dollar. Example: 10000000')

    args = parser.parse_args()
    
    _dollarbarutil.generate_dollarbar(args.exchange, args.symbol, int(args.interval))

if __name__ == "__main__":
    main()