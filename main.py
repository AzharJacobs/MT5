import time
import signal
import sys
from datetime import datetime
from config import config
from database import Database
from logger import Logger
from mt5_connector import MT5Connector
from data_fetcher import DataFetcher

class MarketDataCollector:
    def __init__(self):
        self.running = False
        self.db = None
        self.logger = None
        self.mt5 = None
        self.fetcher = None

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        print("\nShutdown signal received, stopping gracefully...")
        self.stop()

    def initialize(self) -> bool:
        try:
            print("Initializing MT5 Market Data Collector...")

            self.db = Database(config.TURSO_DATABASE_URL, config.TURSO_AUTH_TOKEN)
            print("Turso database connected")

            self.logger = Logger('MT5Collector', self.db)
            self.logger.info("Service starting...")

            self.mt5 = MT5Connector(
                login=config.MT5_LOGIN,
                password=config.MT5_PASSWORD,
                server=config.MT5_SERVER,
                logger=self.logger,
                max_reconnect_attempts=config.MAX_RECONNECT_ATTEMPTS,
                reconnect_delay=config.RECONNECT_DELAY_SECONDS
            )

            if not self.mt5.connect():
                self.logger.error("Failed to connect to MT5")
                return False

            if not self.mt5.test_connection():
                self.logger.error("MT5 connection test failed")
                return False

            self.fetcher = DataFetcher(self.mt5, self.db, self.logger)

            self.logger.info(
                "Initialization complete",
                details={
                    'symbols': config.SYMBOLS,
                    'timeframes': list(config.TIMEFRAMES.keys()),
                    'collection_interval': config.COLLECTION_INTERVAL_SECONDS
                }
            )

            return True

        except Exception as e:
            print(f"Initialization failed: {e}")
            if self.logger:
                self.logger.error(f"Initialization failed: {e}")
            return False

    def run_initial_sync(self):
        self.logger.info("Starting initial historical data sync...")

        for symbol in config.SYMBOLS:
            for timeframe_name, timeframe_minutes in config.TIMEFRAMES.items():
                try:
                    count = self.db.get_candle_count(symbol, timeframe_name)
                    self.logger.info(
                        f"{symbol} {timeframe_name}: {count} existing candles",
                        symbol=symbol,
                        timeframe=timeframe_name
                    )

                    self.fetcher.sync_historical_data(
                        symbol,
                        timeframe_name,
                        config.HISTORICAL_DAYS_LOOKBACK
                    )

                    self.fetcher.detect_and_fill_gaps(
                        symbol,
                        timeframe_name,
                        timeframe_minutes
                    )

                except Exception as e:
                    self.logger.error(
                        f"Error during initial sync for {symbol} {timeframe_name}: {e}",
                        symbol=symbol,
                        timeframe=timeframe_name
                    )

        self.logger.info("Initial sync completed")

    def run_live_collection(self):
        self.logger.info("Starting live data collection...")
        self.running = True

        iteration = 0

        while self.running:
            try:
                iteration += 1
                self.logger.info(f"Collection cycle {iteration} starting...")

                if not self.db.is_connected():
                    self.logger.warning("Database connection lost, reconnecting...")
                    self.db.reconnect()

                if not self.mt5.ensure_connection():
                    self.logger.error("MT5 connection lost and reconnect failed")
                    time.sleep(config.RECONNECT_DELAY_SECONDS)
                    continue

                for symbol in config.SYMBOLS:
                    for timeframe_name, timeframe_minutes in config.TIMEFRAMES.items():
                        try:
                            self.fetcher.collect_live_data(symbol, timeframe_name)

                        except Exception as e:
                            self.logger.error(
                                f"Error collecting live data for {symbol} {timeframe_name}: {e}",
                                symbol=symbol,
                                timeframe=timeframe_name
                            )

                if iteration % 10 == 0:
                    self.logger.info("Running gap detection...")
                    for symbol in config.SYMBOLS:
                        for timeframe_name, timeframe_minutes in config.TIMEFRAMES.items():
                            try:
                                self.fetcher.detect_and_fill_gaps(
                                    symbol,
                                    timeframe_name,
                                    timeframe_minutes
                                )
                            except Exception as e:
                                self.logger.error(
                                    f"Error during gap detection for {symbol} {timeframe_name}: {e}",
                                    symbol=symbol,
                                    timeframe=timeframe_name
                                )

                self.logger.info(f"Collection cycle {iteration} completed, waiting {config.COLLECTION_INTERVAL_SECONDS}s...")
                time.sleep(config.COLLECTION_INTERVAL_SECONDS)

            except Exception as e:
                self.logger.error(f"Error in live collection loop: {e}")
                time.sleep(config.COLLECTION_INTERVAL_SECONDS)

    def stop(self):
        self.logger.info("Stopping service...")
        self.running = False

        if self.mt5:
            self.mt5.disconnect()

        if self.db:
            self.db.close()

        self.logger.info("Service stopped")
        sys.exit(0)

    def run(self):
        if not self.initialize():
            print("Failed to initialize. Exiting.")
            sys.exit(1)

        print("\n" + "="*60)
        print("MT5 Market Data Collector")
        print("="*60)
        print(f"Symbols: {', '.join(config.SYMBOLS)}")
        print(f"Timeframes: {', '.join(config.TIMEFRAMES.keys())}")
        print(f"Collection Interval: {config.COLLECTION_INTERVAL_SECONDS}s")
        print("="*60 + "\n")

        self.run_initial_sync()

        print("\nStarting live data collection...")
        print("Press Ctrl+C to stop\n")

        self.run_live_collection()


if __name__ == "__main__":
    collector = MarketDataCollector()
    collector.run()
