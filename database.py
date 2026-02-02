import libsql_client
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import json

class Database:
    def __init__(self, database_url: str, auth_token: str):
        if not database_url or not database_url.strip():
            raise ValueError("TURSO_DATABASE_URL is empty. Please set it in your .env file.")
        if not auth_token or not auth_token.strip():
            raise ValueError("TURSO_AUTH_TOKEN is empty. Please set it in your .env file.")

        self.database_url = database_url.strip()
        self.auth_token = auth_token.strip()
        self.client = None
        self.connect()

    def connect(self):
        try:
            self.client = libsql_client.create_client(
                url=self.database_url,
                auth_token=self.auth_token
            )
            self._initialize_schema()
        except Exception as e:
            raise Exception(f"Failed to connect to Turso database: {e}")

    def _initialize_schema(self):
        try:
            self.client.execute("""
                CREATE TABLE IF NOT EXISTS candles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume INTEGER NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            self.client.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS candles_symbol_timeframe_timestamp_idx
                ON candles(symbol, timeframe, timestamp)
            """)

            self.client.execute("""
                CREATE INDEX IF NOT EXISTS candles_timestamp_idx ON candles(timestamp)
            """)

            self.client.execute("""
                CREATE INDEX IF NOT EXISTS candles_symbol_timeframe_idx ON candles(symbol, timeframe)
            """)

            self.client.execute("""
                CREATE TABLE IF NOT EXISTS data_collection_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    level TEXT NOT NULL,
                    symbol TEXT,
                    timeframe TEXT,
                    message TEXT NOT NULL,
                    details TEXT
                )
            """)

            self.client.execute("""
                CREATE INDEX IF NOT EXISTS logs_timestamp_idx ON data_collection_logs(timestamp)
            """)

            self.client.execute("""
                CREATE INDEX IF NOT EXISTS logs_level_idx ON data_collection_logs(level)
            """)

        except Exception as e:
            raise Exception(f"Failed to initialize schema: {e}")

    def reconnect(self):
        try:
            if self.client:
                self.client.close()
        except:
            pass
        self.connect()

    def is_connected(self) -> bool:
        try:
            if self.client is None:
                return False
            self.client.execute("SELECT 1")
            return True
        except:
            return False

    def insert_candles(self, candles: List[Dict[str, Any]]) -> int:
        if not candles:
            return 0

        try:
            inserted_count = 0
            for candle in candles:
                timestamp_str = candle['timestamp'].isoformat()

                result = self.client.execute("""
                    INSERT OR IGNORE INTO candles
                    (symbol, timeframe, timestamp, open, high, low, close, volume, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """, [
                    candle['symbol'],
                    candle['timeframe'],
                    timestamp_str,
                    candle['open'],
                    candle['high'],
                    candle['low'],
                    candle['close'],
                    candle['volume']
                ])

                if result.rows_affected > 0:
                    inserted_count += 1

            return inserted_count
        except Exception as e:
            raise Exception(f"Failed to insert candles: {e}")

    def get_last_candle_timestamp(self, symbol: str, timeframe: str) -> Optional[datetime]:
        try:
            result = self.client.execute("""
                SELECT MAX(timestamp) as last_timestamp
                FROM candles
                WHERE symbol = ? AND timeframe = ?
            """, [symbol, timeframe])

            if result.rows and result.rows[0]['last_timestamp']:
                timestamp_str = result.rows[0]['last_timestamp']
                return datetime.fromisoformat(timestamp_str)
            return None
        except Exception as e:
            raise Exception(f"Failed to get last candle timestamp: {e}")

    def get_candle_count(self, symbol: str, timeframe: str) -> int:
        try:
            result = self.client.execute("""
                SELECT COUNT(*) as count FROM candles
                WHERE symbol = ? AND timeframe = ?
            """, [symbol, timeframe])

            if result.rows:
                return result.rows[0]['count']
            return 0
        except Exception as e:
            raise Exception(f"Failed to get candle count: {e}")

    def get_candles_in_range(self, symbol: str, timeframe: str,
                            start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        try:
            start_str = start_time.isoformat()
            end_str = end_time.isoformat()

            result = self.client.execute("""
                SELECT timestamp, open, high, low, close, volume
                FROM candles
                WHERE symbol = ? AND timeframe = ?
                  AND timestamp >= ? AND timestamp < ?
                ORDER BY timestamp ASC
            """, [symbol, timeframe, start_str, end_str])

            candles = []
            for row in result.rows:
                candles.append({
                    'timestamp': datetime.fromisoformat(row['timestamp']),
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close']),
                    'volume': int(row['volume'])
                })
            return candles
        except Exception as e:
            raise Exception(f"Failed to get candles in range: {e}")

    def detect_gaps(self, symbol: str, timeframe: str,
                   start_time: datetime, end_time: datetime,
                   expected_interval_minutes: int) -> List[Tuple[datetime, datetime]]:
        try:
            start_str = start_time.isoformat()
            end_str = end_time.isoformat()

            result = self.client.execute("""
                SELECT timestamp
                FROM candles
                WHERE symbol = ? AND timeframe = ?
                  AND timestamp >= ? AND timestamp < ?
                ORDER BY timestamp
            """, [symbol, timeframe, start_str, end_str])

            if not result.rows or len(result.rows) < 2:
                return []

            gaps = []
            prev_timestamp = None
            expected_interval_seconds = expected_interval_minutes * 60 * 1.5

            for row in result.rows:
                current_timestamp = datetime.fromisoformat(row['timestamp'])

                if prev_timestamp:
                    time_diff = (current_timestamp - prev_timestamp).total_seconds()
                    if time_diff > expected_interval_seconds:
                        gaps.append((prev_timestamp, current_timestamp))

                prev_timestamp = current_timestamp

            return gaps
        except Exception as e:
            raise Exception(f"Failed to detect gaps: {e}")

    def log_event(self, level: str, message: str, symbol: Optional[str] = None,
                 timeframe: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        try:
            details_json = json.dumps(details) if details else None

            self.client.execute("""
                INSERT INTO data_collection_logs (level, symbol, timeframe, message, details, timestamp)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, [level, symbol, timeframe, message, details_json])
        except Exception as e:
            raise Exception(f"Failed to log event: {e}")

    def close(self):
        if self.client:
            try:
                self.client.close()
            except:
                pass
