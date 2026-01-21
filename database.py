import os
import socket
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
import json

class Database:
    def __init__(self, connection_string: str):
        if not connection_string or not connection_string.strip():
            raise ValueError("DATABASE_URL is empty. Please set it in your .env file.")
        self.connection_string = connection_string.strip()
        self.conn = None
        self.connect()

    def connect(self):
        try:
            conn_str = self._prepare_connection_string(self.connection_string)
            # Keep timeouts explicit so a bad network fails fast instead of hanging.
            self.conn = psycopg2.connect(conn_str, connect_timeout=int(os.getenv("DB_CONNECT_TIMEOUT", "10")))
            self.conn.autocommit = False
        except psycopg2.OperationalError as e:
            error_msg = str(e)
            if "could not translate host name" in error_msg.lower():
                raise Exception(f"Failed to resolve database hostname. Check your DATABASE_URL. Error: {e}")
            elif "timeout" in error_msg.lower() or "timed out" in error_msg.lower():
                raise Exception(f"Connection timeout. Try setting DB_FORCE_IPV4=1 in your .env file. Error: {e}")
            elif "password authentication failed" in error_msg.lower():
                raise Exception(f"Authentication failed. Check your database credentials in DATABASE_URL. Error: {e}")
            elif "SSL" in error_msg or "sslmode" in error_msg.lower():
                raise Exception(f"SSL connection error. Supabase requires SSL. Ensure your DATABASE_URL includes '?sslmode=require'. Error: {e}")
            else:
                raise Exception(f"Failed to connect to database: {e}")
        except Exception as e:
            raise Exception(f"Failed to connect to database: {e}")

    def _prepare_connection_string(self, connection_string: str) -> str:
        """
        Prepares the connection string for psycopg2, ensuring SSL is enabled for Supabase
        and optionally forcing IPv4 if needed.
        """
        # Ensure SSL is required for Supabase (if not already specified)
        parsed = urlparse(connection_string)
        query_params = parse_qs(parsed.query)
        
        # If sslmode is not set, add it (Supabase requires SSL)
        if 'sslmode' not in query_params:
            query_params['sslmode'] = ['require']
        
        # Rebuild query string
        new_query = urlencode(query_params, doseq=True)
        
        # Rebuild URL with SSL parameter
        conn_with_ssl = urlunparse(parsed._replace(query=new_query))
        
        # Optionally force IPv4
        return self._maybe_force_ipv4(conn_with_ssl)

    def _maybe_force_ipv4(self, connection_string: str) -> str:
        """
        Some networks have broken IPv6 or block outbound 5432; Supabase hosts often resolve to IPv6 first.
        If DB_FORCE_IPV4=1, resolve the hostname to an IPv4 address and rewrite the connection string
        to use that address (keeps SSL params, query parameters, and credentials intact).
        """
        if os.getenv("DB_FORCE_IPV4", "").lower() not in {"1", "true", "yes"}:
            return connection_string

        try:
            parsed = urlparse(connection_string)
            hostname = parsed.hostname
            if not hostname:
                return connection_string

            # Resolve IPv4 only
            infos = socket.getaddrinfo(hostname, parsed.port or 5432, family=socket.AF_INET, type=socket.SOCK_STREAM)
            if not infos:
                return connection_string
            ipv4 = infos[0][4][0]

            # Rebuild netloc, preserving username/password/port
            userinfo = ""
            if parsed.username:
                userinfo += parsed.username
                if parsed.password:
                    userinfo += f":{parsed.password}"
                userinfo += "@"
            port = f":{parsed.port}" if parsed.port else ""
            new_netloc = f"{userinfo}{ipv4}{port}"

            # Preserve all parts including query parameters (SSL mode, etc.)
            return urlunparse(parsed._replace(netloc=new_netloc))
        except Exception:
            # If anything goes wrong, fall back to the original string.
            return connection_string

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
