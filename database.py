import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
import json

class Database:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.conn = None
        self.connect()

    def connect(self):
        try:
            self.conn = psycopg2.connect(self.connection_string)
            self.conn.autocommit = False
        except Exception as e:
            raise Exception(f"Failed to connect to database: {e}")

    def reconnect(self):
        try:
            if self.conn:
                self.conn.close()
        except:
            pass
        self.connect()

    def is_connected(self) -> bool:
        try:
            if self.conn is None:
                return False
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
                return True
        except:
            return False

    def insert_candles(self, candles: List[Dict[str, Any]]) -> int:
        if not candles:
            return 0

        query = """
            INSERT INTO candles (symbol, timeframe, timestamp, open, high, low, close, volume)
            VALUES %s
            ON CONFLICT (symbol, timeframe, timestamp) DO NOTHING
        """

        try:
            with self.conn.cursor() as cur:
                values = [
                    (
                        candle['symbol'],
                        candle['timeframe'],
                        candle['timestamp'],
                        candle['open'],
                        candle['high'],
                        candle['low'],
                        candle['close'],
                        candle['volume']
                    )
                    for candle in candles
                ]
                execute_values(cur, query, values)
                self.conn.commit()
                return len(candles)
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Failed to insert candles: {e}")

    def get_last_candle_timestamp(self, symbol: str, timeframe: str) -> Optional[datetime]:
        query = """
            SELECT MAX(timestamp) as last_timestamp
            FROM candles
            WHERE symbol = %s AND timeframe = %s
        """

        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (symbol, timeframe))
                result = cur.fetchone()
                return result[0] if result and result[0] else None
        except Exception as e:
            raise Exception(f"Failed to get last candle timestamp: {e}")

    def get_candle_count(self, symbol: str, timeframe: str) -> int:
        query = """
            SELECT COUNT(*) FROM candles
            WHERE symbol = %s AND timeframe = %s
        """

        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (symbol, timeframe))
                result = cur.fetchone()
                return result[0] if result else 0
        except Exception as e:
            raise Exception(f"Failed to get candle count: {e}")

    def get_candles_in_range(self, symbol: str, timeframe: str,
                            start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        query = """
            SELECT timestamp, open, high, low, close, volume
            FROM candles
            WHERE symbol = %s AND timeframe = %s
              AND timestamp >= %s AND timestamp < %s
            ORDER BY timestamp ASC
        """

        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (symbol, timeframe, start_time, end_time))
                results = cur.fetchall()
                return [
                    {
                        'timestamp': row[0],
                        'open': float(row[1]),
                        'high': float(row[2]),
                        'low': float(row[3]),
                        'close': float(row[4]),
                        'volume': int(row[5])
                    }
                    for row in results
                ]
        except Exception as e:
            raise Exception(f"Failed to get candles in range: {e}")

    def detect_gaps(self, symbol: str, timeframe: str,
                   start_time: datetime, end_time: datetime,
                   expected_interval_minutes: int) -> List[Tuple[datetime, datetime]]:
        query = """
            WITH candle_times AS (
                SELECT timestamp,
                       LAG(timestamp) OVER (ORDER BY timestamp) as prev_timestamp
                FROM candles
                WHERE symbol = %s AND timeframe = %s
                  AND timestamp >= %s AND timestamp < %s
                ORDER BY timestamp
            )
            SELECT prev_timestamp, timestamp
            FROM candle_times
            WHERE prev_timestamp IS NOT NULL
              AND EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) > %s * 60 * 1.5
            ORDER BY prev_timestamp
        """

        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (symbol, timeframe, start_time, end_time, expected_interval_minutes))
                results = cur.fetchall()
                return [(row[0], row[1]) for row in results]
        except Exception as e:
            raise Exception(f"Failed to detect gaps: {e}")

    def log_event(self, level: str, message: str, symbol: Optional[str] = None,
                 timeframe: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        query = """
            INSERT INTO data_collection_logs (level, symbol, timeframe, message, details)
            VALUES (%s, %s, %s, %s, %s)
        """

        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (
                    level,
                    symbol,
                    timeframe,
                    message,
                    json.dumps(details) if details else None
                ))
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Failed to log event: {e}")

    def close(self):
        if self.conn:
            self.conn.close()
