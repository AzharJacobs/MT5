import MetaTrader5 as mt5
from datetime import datetime, timedelta
import pytz
from typing import List, Dict, Any, Optional, Tuple
from mt5_connector import MT5Connector
from database import Database
from logger import Logger

class DataFetcher:
    TIMEFRAME_MAP = {
        'M1': mt5.TIMEFRAME_M1,
        'M5': mt5.TIMEFRAME_M5,
        'M15': mt5.TIMEFRAME_M15,
        'M30': mt5.TIMEFRAME_M30,
        'H1': mt5.TIMEFRAME_H1,
        'H4': mt5.TIMEFRAME_H4,
        'D1': mt5.TIMEFRAME_D1,
    }
    TIMEFRAME_MINUTES = {
        'M1': 1,
        'M5': 5,
        'M15': 15,
        'M30': 30,
        'H1': 60,
        'H4': 240,
        'D1': 1440,
    }

    def __init__(self, mt5_connector: MT5Connector, db: Database, logger: Logger):
        self.mt5 = mt5_connector
        self.db = db
        self.logger = logger

    def _convert_to_utc(self, dt: datetime) -> datetime:
        if dt.tzinfo is None:
            dt = pytz.UTC.localize(dt)
        return dt.astimezone(pytz.UTC)

    def _get_mt5_timeframe(self, timeframe: str):
        return self.TIMEFRAME_MAP.get(timeframe)

    def _get_timeframe_minutes(self, timeframe: str) -> Optional[int]:
        return self.TIMEFRAME_MINUTES.get(timeframe)

    def fetch_historical_data(self, symbol: str, timeframe: str,
                            start_date: datetime, end_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        if not self.mt5.ensure_connection():
            self.logger.error(f"Cannot fetch data - MT5 not connected", symbol=symbol, timeframe=timeframe)
            return []

        mt5_symbol = self.mt5.resolve_symbol(symbol)
        if not mt5_symbol:
            self.logger.error(f"Symbol {symbol} not available", symbol=symbol, timeframe=timeframe)
            return []
        # Ensure it's selected/visible in Market Watch
        if not self.mt5.get_symbol_info(symbol):
            self.logger.error(f"Symbol {symbol} not available", symbol=symbol, timeframe=timeframe)
            return []

        mt5_timeframe = self._get_mt5_timeframe(timeframe)
        if mt5_timeframe is None:
            self.logger.error(f"Invalid timeframe: {timeframe}", symbol=symbol, timeframe=timeframe)
            return []

        try:
            start_utc = self._convert_to_utc(start_date)
            end_utc = self._convert_to_utc(end_date) if end_date else self._convert_to_utc(datetime.now())

            self.logger.info(
                f"Fetching historical data for {symbol} {timeframe} from {start_utc} to {end_utc}",
                symbol=symbol,
                timeframe=timeframe
            )

            # Fetch in chunks to avoid MT5 "Invalid params" on large ranges (esp. M1/M5).
            tf_minutes = self._get_timeframe_minutes(timeframe) or 1
            max_bars_per_call = 50000
            chunk_delta = timedelta(minutes=tf_minutes * max_bars_per_call)

            all_candles: List[Dict[str, Any]] = []
            chunk_start = start_utc
            while chunk_start < end_utc:
                chunk_end = min(end_utc, chunk_start + chunk_delta)
                rates = mt5.copy_rates_range(mt5_symbol, mt5_timeframe, chunk_start, chunk_end)

                if rates is None or len(rates) == 0:
                    error = mt5.last_error()
                    # If the call fails due to params, tighten the chunk and retry.
                    if "Invalid params" in str(error) and chunk_delta > timedelta(days=1):
                        chunk_delta = max(timedelta(days=1), chunk_delta / 2)
                        continue
                    self.logger.warning(
                        f"No data received for {symbol} {timeframe}: {error}",
                        symbol=symbol,
                        timeframe=timeframe
                    )
                else:
                    all_candles.extend(self._convert_rates_to_candles(rates, symbol, timeframe))

                chunk_start = chunk_end

            candles = all_candles
            self.logger.info(
                f"Fetched {len(candles)} candles for {symbol} {timeframe}",
                symbol=symbol,
                timeframe=timeframe,
                details={'count': len(candles)}
            )

            return candles

        except Exception as e:
            self.logger.error(
                f"Error fetching historical data for {symbol} {timeframe}: {e}",
                symbol=symbol,
                timeframe=timeframe
            )
            return []

    def fetch_latest_candles(self, symbol: str, timeframe: str, count: int = 100) -> List[Dict[str, Any]]:
        if not self.mt5.ensure_connection():
            self.logger.error(f"Cannot fetch data - MT5 not connected", symbol=symbol, timeframe=timeframe)
            return []

        mt5_symbol = self.mt5.resolve_symbol(symbol)
        if not mt5_symbol:
            self.logger.error(f"Symbol {symbol} not available", symbol=symbol, timeframe=timeframe)
            return []
        # Ensure it's selected/visible in Market Watch
        if not self.mt5.get_symbol_info(symbol):
            self.logger.error(f"Symbol {symbol} not available", symbol=symbol, timeframe=timeframe)
            return []

        mt5_timeframe = self._get_mt5_timeframe(timeframe)
        if mt5_timeframe is None:
            self.logger.error(f"Invalid timeframe: {timeframe}", symbol=symbol, timeframe=timeframe)
            return []

        try:
            rates = mt5.copy_rates_from_pos(mt5_symbol, mt5_timeframe, 0, count)

            if rates is None or len(rates) == 0:
                error = mt5.last_error()
                self.logger.warning(
                    f"No latest data received for {symbol} {timeframe}: {error}",
                    symbol=symbol,
                    timeframe=timeframe
                )
                return []

            candles = self._convert_rates_to_candles(rates, symbol, timeframe)
            return candles

        except Exception as e:
            self.logger.error(
                f"Error fetching latest candles for {symbol} {timeframe}: {e}",
                symbol=symbol,
                timeframe=timeframe
            )
            return []

    def _convert_rates_to_candles(self, rates, symbol: str, timeframe: str) -> List[Dict[str, Any]]:
        candles = []
        tf_minutes = self._get_timeframe_minutes(timeframe) or 0
        for rate in rates:
            timestamp = datetime.fromtimestamp(rate['time'], tz=pytz.UTC)
            candle = {
                'symbol': symbol,
                'timeframe': timeframe,
                'timeframe_minutes': tf_minutes,
                'timestamp': timestamp,
                'open': float(rate['open']),
                'high': float(rate['high']),
                'low': float(rate['low']),
                'close': float(rate['close']),
                'volume': int(rate['tick_volume'])
            }
            candles.append(candle)
        return candles

    def sync_historical_data(self, symbol: str, timeframe: str, days_back: int = 365):
        self.logger.info(
            f"Starting historical sync for {symbol} {timeframe} ({days_back} days)",
            symbol=symbol,
            timeframe=timeframe
        )

        last_timestamp = self.db.get_last_candle_timestamp(symbol, timeframe)

        if last_timestamp:
            start_date = last_timestamp - timedelta(days=1)
            self.logger.info(
                f"Resuming from last candle: {last_timestamp}",
                symbol=symbol,
                timeframe=timeframe
            )
        else:
            start_date = datetime.now(pytz.UTC) - timedelta(days=days_back)
            self.logger.info(
                f"No existing data, fetching from {start_date}",
                symbol=symbol,
                timeframe=timeframe
            )

        candles = self.fetch_historical_data(symbol, timeframe, start_date)

        if candles:
            inserted = self.db.insert_candles(candles)
            self.logger.info(
                f"Historical sync complete: {inserted} new candles inserted",
                symbol=symbol,
                timeframe=timeframe,
                details={'inserted': inserted, 'total_fetched': len(candles)}
            )
        else:
            self.logger.warning(
                f"No historical data to sync",
                symbol=symbol,
                timeframe=timeframe
            )

    def detect_and_fill_gaps(self, symbol: str, timeframe: str, timeframe_minutes: int):
        self.logger.info(
            f"Checking for gaps in {symbol} {timeframe}",
            symbol=symbol,
            timeframe=timeframe
        )

        last_timestamp = self.db.get_last_candle_timestamp(symbol, timeframe)
        if not last_timestamp:
            self.logger.info(
                f"No data exists for gap detection",
                symbol=symbol,
                timeframe=timeframe
            )
            return

        start_time = last_timestamp - timedelta(days=30)
        end_time = datetime.now(pytz.UTC)

        gaps = self.db.detect_gaps(symbol, timeframe, start_time, end_time, timeframe_minutes)

        if not gaps:
            self.logger.info(
                f"No gaps detected",
                symbol=symbol,
                timeframe=timeframe
            )
            return

        self.logger.warning(
            f"Detected {len(gaps)} gaps in data",
            symbol=symbol,
            timeframe=timeframe,
            details={'gap_count': len(gaps)}
        )

        for gap_start, gap_end in gaps:
            self.logger.info(
                f"Filling gap from {gap_start} to {gap_end}",
                symbol=symbol,
                timeframe=timeframe
            )

            candles = self.fetch_historical_data(symbol, timeframe, gap_start, gap_end)
            if candles:
                inserted = self.db.insert_candles(candles)
                self.logger.info(
                    f"Gap filled: {inserted} candles inserted",
                    symbol=symbol,
                    timeframe=timeframe,
                    details={'gap_start': str(gap_start), 'gap_end': str(gap_end), 'inserted': inserted}
                )

    def collect_live_data(self, symbol: str, timeframe: str):
        candles = self.fetch_latest_candles(symbol, timeframe, count=10)

        if candles:
            inserted = self.db.insert_candles(candles)
            if inserted > 0:
                self.logger.info(
                    f"Live data collected: {inserted} new candles",
                    symbol=symbol,
                    timeframe=timeframe,
                    details={'inserted': inserted}
                )
        else:
            self.logger.warning(
                f"No live data collected",
                symbol=symbol,
                timeframe=timeframe
            )
