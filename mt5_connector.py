import MetaTrader5 as mt5
import time
from typing import Optional, Dict, List, Tuple
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
        # Cache requested_symbol -> resolved broker symbol name (or None)
        self._symbol_cache: Dict[str, Optional[str]] = {}

    @staticmethod
    def _norm_symbol(s: str) -> str:
        return "".join(ch.lower() for ch in (s or "") if ch.isalnum())

    def _list_all_symbols(self) -> List[str]:
        try:
            syms = mt5.symbols_get()
            if not syms:
                return []
            return [s.name for s in syms if getattr(s, "name", None)]
        except Exception:
            return []

    def resolve_symbol(self, requested_symbol: str) -> Optional[str]:
        """
        Resolve a user-friendly symbol (e.g. US30, USTech) to the broker's actual
        MT5 symbol name (e.g. US30.cash, USTEC, NAS100, etc.).
        Returns the MT5 symbol name to use in API calls, or None if not found.
        """
        if not requested_symbol:
            return None

        if requested_symbol in self._symbol_cache:
            return self._symbol_cache[requested_symbol]

        if not self.ensure_connection():
            self._symbol_cache[requested_symbol] = None
            return None

        # 1) Exact match
        info = mt5.symbol_info(requested_symbol)
        if info is not None:
            self._symbol_cache[requested_symbol] = requested_symbol
            return requested_symbol

        # 2) Heuristic/alias match across all available symbols
        requested_norm = self._norm_symbol(requested_symbol)
        alias_map = {
            # Common broker aliases
            "us30": ["us30", "dj30", "dow", "wallstreet", "ws30", "us30cash", "us30m", "us30z"],
            "ustech": ["ustech", "nas100", "nas", "nasdaq", "ustec", "us100", "tech100", "nq100"],
        }

        candidates: List[Tuple[int, str]] = []
        for name in self._list_all_symbols():
            name_norm = self._norm_symbol(name)
            score = 0

            # direct contains/startswith
            if requested_norm and requested_norm == name_norm:
                score = 100
            elif requested_norm and name_norm.startswith(requested_norm):
                score = 90
            elif requested_norm and requested_norm in name_norm:
                score = 80

            # alias boosts
            for alias in alias_map.get(requested_norm, []):
                a = self._norm_symbol(alias)
                if not a:
                    continue
                if name_norm == a:
                    score = max(score, 98)
                elif name_norm.startswith(a):
                    score = max(score, 88)
                elif a in name_norm:
                    score = max(score, 78)

            if score > 0:
                # Slightly prefer shorter names if same score
                candidates.append((score * 1000 - len(name), name))

        candidates.sort(reverse=True)
        resolved = candidates[0][1] if candidates else None

        if resolved:
            self.logger.warning(
                f"Symbol '{requested_symbol}' not found; using broker symbol '{resolved}'",
                symbol=requested_symbol,
                details={"requested": requested_symbol, "resolved": resolved},
            )
        else:
            # Provide a few hints for debugging
            sample = self._list_all_symbols()[:25]
            self.logger.error(
                f"Symbol '{requested_symbol}' not found on this account/broker",
                symbol=requested_symbol,
                details={"sample_symbols": sample},
            )

        self._symbol_cache[requested_symbol] = resolved
        return resolved

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

            mt5_symbol = self.resolve_symbol(symbol)
            if not mt5_symbol:
                return None

            symbol_info = mt5.symbol_info(mt5_symbol)
            if symbol_info is None:
                self.logger.error(f"Symbol {symbol} not found", symbol=symbol)
                return None

            if not symbol_info.visible:
                if not mt5.symbol_select(mt5_symbol, True):
                    self.logger.error(f"Failed to select symbol {mt5_symbol}", symbol=symbol)
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
