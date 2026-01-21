import logging
import sys
from datetime import datetime
from typing import Optional, Dict, Any
from database import Database

class Logger:
    def __init__(self, name: str, db: Optional[Database] = None):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        self.db = db

    def _log_to_db(self, level: str, message: str, symbol: Optional[str] = None,
                   timeframe: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        if self.db:
            try:
                self.db.log_event(level, message, symbol, timeframe, details)
            except Exception as e:
                self.logger.error(f"Failed to log to database: {e}")

    def info(self, message: str, symbol: Optional[str] = None,
             timeframe: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        self.logger.info(message)
        self._log_to_db('INFO', message, symbol, timeframe, details)

    def warning(self, message: str, symbol: Optional[str] = None,
                timeframe: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        self.logger.warning(message)
        self._log_to_db('WARNING', message, symbol, timeframe, details)

    def error(self, message: str, symbol: Optional[str] = None,
              timeframe: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        self.logger.error(message)
        self._log_to_db('ERROR', message, symbol, timeframe, details)

    def debug(self, message: str):
        self.logger.debug(message)
