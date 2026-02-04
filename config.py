import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    MT5_LOGIN = int(os.getenv('MT5_LOGIN', 0))
    MT5_PASSWORD = os.getenv('MT5_PASSWORD', '')
    MT5_SERVER = os.getenv('MT5_SERVER', '')

    MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
    MYSQL_PORT = int(os.getenv('MYSQL_PORT', '3306'))
    MYSQL_USER = os.getenv('MYSQL_USER', 'root')
    MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '')
    MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'trading_app_1')

    SYMBOLS = ['US30', 'USTech']

    TIMEFRAMES = {
        'M1': 1,
        'M5': 5,
        'M15': 15,
        'M30': 30,
        'H1': 60,
        'H4': 240,
        'D1': 1440,
    }

    COLLECTION_INTERVAL_SECONDS = 60

    MAX_RECONNECT_ATTEMPTS = 5
    RECONNECT_DELAY_SECONDS = 10

    HISTORICAL_DAYS_LOOKBACK = 365

    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

config = Config()
