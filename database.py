"""
Reusable MySQL database connection module for the trading app.
Uses mysql-connector-python with connection parameters from config (env for password).
"""
import mysql.connector
from mysql.connector import Error as MySQLError
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
import json


def get_connection(
    host: str = "localhost",
    port: int = 3306,
    user: str = "root",
    password: str = "",
    database: str = "trading_app_1",
    **kwargs,
) -> mysql.connector.MySQLConnection:
    """
    Create and return a MySQL connection. Use for one-off scripts or
    when you need a fresh connection. For the Database class, the
    connection is managed internally.
    """
    return mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        **kwargs,
    )


class Database:
    """MySQL-backed storage for candles and data collection logs."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 3306,
        user: str = "root",
        password: str = "",
        database: str = "trading_app_1",
    ):
        if not host or not host.strip():
            raise ValueError("MySQL host is required.")
        if not user or not user.strip():
            raise ValueError("MySQL user is required.")
        if not database or not database.strip():
            raise ValueError("MySQL database name is required.")

        self.host = host.strip()
        self.port = int(port)
        self.user = user.strip()
        self.password = (password or "").strip()
        self.database = database.strip()
        self._conn = None
        self.connect()

    def _create_index_if_missing(self, cur, table: str, index_name: str, columns: str) -> None:
        """Create index only if it does not exist (MySQL has no CREATE INDEX IF NOT EXISTS)."""
        cur.execute(
            """
            SELECT 1 FROM information_schema.statistics
            WHERE table_schema = %s AND table_name = %s AND index_name = %s
            LIMIT 1
            """,
            [self.database, table, index_name],
        )
        if cur.fetchone():
            return
        cur.execute(f"CREATE INDEX {index_name} ON {table} {columns}")

    def _column_exists(self, cur, table: str, column: str) -> bool:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
            LIMIT 1
            """,
            [self.database, table, column],
        )
        return cur.fetchone() is not None

    def _ensure_column(self, cur, table: str, column: str, add_sql: str) -> None:
        """Add a column if it doesn't exist (safe for older schemas)."""
        if self._column_exists(cur, table, column):
            return
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {add_sql}")

    def _ensure_candles_required_columns(self, cur) -> None:
        """
        Ensure `candles` has all columns expected by the app.
        This makes upgrades from older schemas safe.
        """
        # Core identifying columns
        self._ensure_column(cur, "candles", "symbol", "symbol VARCHAR(64) NULL")
        self._ensure_column(cur, "candles", "timeframe", "timeframe VARCHAR(32) NULL")

        # Candle data columns
        self._ensure_column(cur, "candles", "open", "`open` DOUBLE NULL")
        self._ensure_column(cur, "candles", "high", "high DOUBLE NULL")
        self._ensure_column(cur, "candles", "low", "low DOUBLE NULL")
        self._ensure_column(cur, "candles", "close", "`close` DOUBLE NULL")
        self._ensure_column(cur, "candles", "volume", "volume BIGINT NULL")

        # Timestamps/metadata
        self._ensure_column(cur, "candles", "created_at", "created_at DATETIME NULL")
        self._ensure_column(cur, "candles", "updated_at", "updated_at DATETIME NULL")

        # Backfill NULLs to stable defaults, then enforce NOT NULL where the app depends on it.
        cur.execute("UPDATE candles SET symbol = '' WHERE symbol IS NULL")
        cur.execute("UPDATE candles SET timeframe = '' WHERE timeframe IS NULL")
        cur.execute("UPDATE candles SET `open` = 0 WHERE `open` IS NULL")
        cur.execute("UPDATE candles SET high = 0 WHERE high IS NULL")
        cur.execute("UPDATE candles SET low = 0 WHERE low IS NULL")
        cur.execute("UPDATE candles SET `close` = 0 WHERE `close` IS NULL")
        cur.execute("UPDATE candles SET volume = 0 WHERE volume IS NULL")

        # Use CURRENT_TIMESTAMP for missing created/updated values.
        cur.execute("UPDATE candles SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL")
        cur.execute("UPDATE candles SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL")

        cur.execute("ALTER TABLE candles MODIFY COLUMN symbol VARCHAR(64) NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE candles MODIFY COLUMN timeframe VARCHAR(32) NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE candles MODIFY COLUMN `open` DOUBLE NOT NULL DEFAULT 0")
        cur.execute("ALTER TABLE candles MODIFY COLUMN high DOUBLE NOT NULL DEFAULT 0")
        cur.execute("ALTER TABLE candles MODIFY COLUMN low DOUBLE NOT NULL DEFAULT 0")
        cur.execute("ALTER TABLE candles MODIFY COLUMN `close` DOUBLE NOT NULL DEFAULT 0")
        cur.execute("ALTER TABLE candles MODIFY COLUMN volume BIGINT NOT NULL DEFAULT 0")
        cur.execute("ALTER TABLE candles MODIFY COLUMN created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP")
        cur.execute(
            "ALTER TABLE candles MODIFY COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
        )

    def _ensure_candles_timestamp_column(self, cur) -> None:
        """
        Older installs may have a `candles` table without a `timestamp` column.
        Ensure it exists so index creation and queries don't fail.
        """
        if self._column_exists(cur, "candles", "timestamp"):
            return

        # Add as nullable first to avoid failing on existing rows.
        cur.execute("ALTER TABLE candles ADD COLUMN `timestamp` VARCHAR(64) NULL")

        # If an older column likely contains candle time, copy it across.
        for legacy_col in ("time", "datetime", "date", "candle_time"):
            if self._column_exists(cur, "candles", legacy_col):
                cur.execute(f"UPDATE candles SET `timestamp` = `{legacy_col}` WHERE `timestamp` IS NULL")
                break

        # Make it non-nullable with an empty-string default to keep app logic stable.
        cur.execute("UPDATE candles SET `timestamp` = '' WHERE `timestamp` IS NULL")
        cur.execute("ALTER TABLE candles MODIFY COLUMN `timestamp` VARCHAR(64) NOT NULL DEFAULT ''")

    def connect(self) -> None:
        try:
            self._conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                autocommit=True,
            )
            self._initialize_schema()
        except MySQLError as e:
            raise Exception(f"Failed to connect to MySQL database: {e}") from e

    def _initialize_schema(self) -> None:
        try:
            with self._conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS candles (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        symbol VARCHAR(64) NOT NULL,
                        timeframe VARCHAR(32) NOT NULL,
                        `timestamp` VARCHAR(64) NOT NULL,
                        `open` DOUBLE NOT NULL,
                        high DOUBLE NOT NULL,
                        low DOUBLE NOT NULL,
                        `close` DOUBLE NOT NULL,
                        volume BIGINT NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY candles_symbol_timeframe_timestamp_idx (symbol, timeframe, `timestamp`)
                    )
                """)
                self._ensure_candles_required_columns(cur)
                self._ensure_candles_timestamp_column(cur)
                self._create_index_if_missing(cur, "candles", "candles_timestamp_idx", "(`timestamp`)")
                self._create_index_if_missing(cur, "candles", "candles_symbol_timeframe_idx", "(symbol, timeframe)")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS data_collection_logs (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        `timestamp` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        level VARCHAR(32) NOT NULL,
                        symbol VARCHAR(64) NULL,
                        timeframe VARCHAR(32) NULL,
                        message TEXT NOT NULL,
                        details TEXT NULL
                    )
                """)
                self._create_index_if_missing(cur, "data_collection_logs", "logs_timestamp_idx", "(`timestamp`)")
                self._create_index_if_missing(cur, "data_collection_logs", "logs_level_idx", "(level)")
            self._conn.commit()
        except MySQLError as e:
            raise Exception(f"Failed to initialize schema: {e}") from e

    def reconnect(self) -> None:
        try:
            if self._conn and self._conn.is_connected():
                self._conn.close()
        except Exception:
            pass
        self._conn = None
        self.connect()

    def is_connected(self) -> bool:
        try:
            if self._conn is None:
                return False
            if not self._conn.is_connected():
                return False
            cur = self._conn.cursor()
            try:
                cur.execute("SELECT 1")
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
            return True
        except Exception:
            return False

    def insert_candles(self, candles: List[Dict[str, Any]]) -> int:
        if not candles:
            return 0

        inserted_count = 0
        try:
            with self._conn.cursor(dictionary=True) as cur:
                for candle in candles:
                    timestamp_str = candle["timestamp"].isoformat()
                    cur.execute(
                        """
                        INSERT IGNORE INTO candles
                        (symbol, timeframe, `timestamp`, `open`, high, low, `close`, volume, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """,
                        [
                            candle["symbol"],
                            candle["timeframe"],
                            timestamp_str,
                            candle["open"],
                            candle["high"],
                            candle["low"],
                            candle["close"],
                            candle["volume"],
                        ],
                    )
                    if cur.rowcount > 0:
                        inserted_count += 1
            self._conn.commit()
            return inserted_count
        except MySQLError as e:
            raise Exception(f"Failed to insert candles: {e}") from e

    def get_last_candle_timestamp(self, symbol: str, timeframe: str) -> Optional[datetime]:
        try:
            with self._conn.cursor(dictionary=True) as cur:
                cur.execute(
                    """
                    SELECT MAX(`timestamp`) AS last_timestamp
                    FROM candles
                    WHERE symbol = %s AND timeframe = %s
                    """,
                    [symbol, timeframe],
                )
                row = cur.fetchone()
                if row and row.get("last_timestamp"):
                    ts = row["last_timestamp"]
                    if isinstance(ts, datetime):
                        return ts
                    return datetime.fromisoformat(str(ts))
                return None
        except MySQLError as e:
            raise Exception(f"Failed to get last candle timestamp: {e}") from e

    def get_candle_count(self, symbol: str, timeframe: str) -> int:
        try:
            with self._conn.cursor(dictionary=True) as cur:
                cur.execute(
                    "SELECT COUNT(*) AS `count` FROM candles WHERE symbol = %s AND timeframe = %s",
                    [symbol, timeframe],
                )
                row = cur.fetchone()
                return int(row["count"]) if row else 0
        except MySQLError as e:
            raise Exception(f"Failed to get candle count: {e}") from e

    def get_candles_in_range(
        self,
        symbol: str,
        timeframe: str,
        start_time: datetime,
        end_time: datetime,
    ) -> List[Dict[str, Any]]:
        try:
            start_str = start_time.isoformat()
            end_str = end_time.isoformat()
            with self._conn.cursor(dictionary=True) as cur:
                cur.execute(
                    """
                    SELECT `timestamp`, `open`, high, low, `close`, volume
                    FROM candles
                    WHERE symbol = %s AND timeframe = %s
                      AND `timestamp` >= %s AND `timestamp` < %s
                    ORDER BY `timestamp` ASC
                    """,
                    [symbol, timeframe, start_str, end_str],
                )
                rows = cur.fetchall()
            candles = []
            for row in rows:
                ts = row["timestamp"]
                if isinstance(ts, datetime):
                    ts = ts.isoformat()
                candles.append(
                    {
                        "timestamp": datetime.fromisoformat(str(ts)),
                        "open": float(row["open"]),
                        "high": float(row["high"]),
                        "low": float(row["low"]),
                        "close": float(row["close"]),
                        "volume": int(row["volume"]),
                    }
                )
            return candles
        except MySQLError as e:
            raise Exception(f"Failed to get candles in range: {e}") from e

    def detect_gaps(
        self,
        symbol: str,
        timeframe: str,
        start_time: datetime,
        end_time: datetime,
        expected_interval_minutes: int,
    ) -> List[Tuple[datetime, datetime]]:
        try:
            start_str = start_time.isoformat()
            end_str = end_time.isoformat()
            with self._conn.cursor(dictionary=True) as cur:
                cur.execute(
                    """
                    SELECT `timestamp`
                    FROM candles
                    WHERE symbol = %s AND timeframe = %s
                      AND `timestamp` >= %s AND `timestamp` < %s
                    ORDER BY `timestamp`
                    """,
                    [symbol, timeframe, start_str, end_str],
                )
                rows = cur.fetchall()

            if not rows or len(rows) < 2:
                return []

            gaps = []
            prev_timestamp = None
            expected_interval_seconds = expected_interval_minutes * 60 * 1.5

            for row in rows:
                ts = row["timestamp"]
                if isinstance(ts, datetime):
                    current_timestamp = ts
                else:
                    current_timestamp = datetime.fromisoformat(str(ts))

                if prev_timestamp is not None:
                    time_diff = (current_timestamp - prev_timestamp).total_seconds()
                    if time_diff > expected_interval_seconds:
                        gaps.append((prev_timestamp, current_timestamp))

                prev_timestamp = current_timestamp

            return gaps
        except MySQLError as e:
            raise Exception(f"Failed to detect gaps: {e}") from e

    def log_event(
        self,
        level: str,
        message: str,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        try:
            details_json = json.dumps(details) if details else None
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO data_collection_logs (level, symbol, timeframe, message, details, `timestamp`)
                    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    """,
                    [level, symbol, timeframe, message, details_json],
                )
            self._conn.commit()
        except MySQLError as e:
            raise Exception(f"Failed to log event: {e}") from e

    def close(self) -> None:
        if self._conn:
            try:
                if self._conn.is_connected():
                    self._conn.close()
            except Exception:
                pass
            self._conn = None
