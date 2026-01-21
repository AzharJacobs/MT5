import MetaTrader5 as mt5
import time
from typing import Optional
from logger import Logger

class MT5Connector:
    def __init__(self, login: int, password: str, server: str, logger: Logger,
                 max_reconnect_attempts: int = 5, reconnect_delay: int = 10):
        self.login = login
        self.password = password
        self.server = server
        self.logger = logger
        self.max_reconnect_attempts = max_reconnect_attempts
        self.reconnect_delay = reconnect_delay
        self.connected = False

    def connect(self) -> bool:
        try:
            if not mt5.initialize():
                error = mt5.last_error()
                self.logger.error(f"MT5 initialize failed: {error}")
                return False

            if not mt5.login(self.login, self.password, self.server):
                error = mt5.last_error()
                self.logger.error(f"MT5 login failed: {error}")
                mt5.shutdown()
                return False

            account_info = mt5.account_info()
            if account_info is None:
                self.logger.error("Failed to get account info")
                mt5.shutdown()
                return False

            self.connected = True
            self.logger.info(
                f"Connected to MT5 account {account_info.login} on {account_info.server}",
                details={
                    'account': account_info.login,
                    'server': account_info.server,
                    'balance': account_info.balance,
                    'currency': account_info.currency
                }
            )
            return True

        except Exception as e:
            self.logger.error(f"Exception during MT5 connection: {e}")
            self.connected = False
            return False

    def disconnect(self):
        try:
            if self.connected:
                mt5.shutdown()
                self.connected = False
                self.logger.info("Disconnected from MT5")
        except Exception as e:
            self.logger.error(f"Error during MT5 disconnect: {e}")

    def ensure_connection(self) -> bool:
        if self.is_connected():
            return True

        self.logger.warning("MT5 connection lost, attempting to reconnect...")
        return self.reconnect()

    def is_connected(self) -> bool:
        try:
            if not self.connected:
                return False

            account_info = mt5.account_info()
            if account_info is None:
                self.connected = False
                return False

            return True
        except:
            self.connected = False
            return False

    def reconnect(self) -> bool:
        self.disconnect()

        for attempt in range(1, self.max_reconnect_attempts + 1):
            self.logger.info(f"Reconnection attempt {attempt}/{self.max_reconnect_attempts}")

            if self.connect():
                self.logger.info("Reconnection successful")
                return True

            if attempt < self.max_reconnect_attempts:
                self.logger.warning(f"Reconnection attempt {attempt} failed, waiting {self.reconnect_delay}s...")
                time.sleep(self.reconnect_delay)

        self.logger.error(f"Failed to reconnect after {self.max_reconnect_attempts} attempts")
        return False

    def get_symbol_info(self, symbol: str) -> Optional[object]:
        try:
            if not self.ensure_connection():
                return None

            symbol_info = mt5.symbol_info(symbol)
            if symbol_info is None:
                self.logger.error(f"Symbol {symbol} not found", symbol=symbol)
                return None

            if not symbol_info.visible:
                if not mt5.symbol_select(symbol, True):
                    self.logger.error(f"Failed to select symbol {symbol}", symbol=symbol)
                    return None

            return symbol_info

        except Exception as e:
            self.logger.error(f"Error getting symbol info for {symbol}: {e}", symbol=symbol)
            return None

    def test_connection(self) -> bool:
        try:
            if not self.ensure_connection():
                return False

            terminal_info = mt5.terminal_info()
            if terminal_info is None:
                self.logger.error("Failed to get terminal info")
                return False

            self.logger.info(
                f"MT5 terminal connected: {terminal_info.name}",
                details={
                    'name': terminal_info.name,
                    'company': terminal_info.company,
                    'path': terminal_info.path
                }
            )
            return True

        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
