# MT5 Market Data Collection Service

A reliable, production-ready service for collecting OHLCV (Open, High, Low, Close, Volume) candle data from MetaTrader 5 (MT5) trading accounts and storing it in a local MySQL database (e.g. MySQL Workbench / MySQL Server).

## Purpose

This service is designed **exclusively for data ingestion**. It:
- Connects to your MT5 trading account
- Fetches historical and live market data
- Detects and fills data gaps
- Stores clean, normalized data in MySQL
- Provides a stable data source for separate ML/strategy applications

**What this service does NOT do:**
- ❌ Execute trades
- ❌ Implement trading strategies
- ❌ Perform machine learning
- ❌ Make trading decisions

## Features

### Core Capabilities
- **Multi-Symbol Support**: Currently configured for US30 and USTech
- **Multi-Timeframe**: Supports M1, M5, M15, M30, H1, H4, D1
- **Historical Data Sync**: Fetches up to 365 days of historical data
- **Live Data Collection**: Continuously collects new candles
- **Gap Detection**: Automatically identifies and backfills missing data
- **UTC Normalization**: All timestamps stored in UTC
- **Idempotent Writes**: Prevents duplicate candle entries
- **Automatic Reconnection**: Handles MT5 and database connection failures

### Reliability Features
- Database connection monitoring and auto-reconnect
- MT5 connection monitoring with configurable retry logic
- Comprehensive error logging to both console and database
- Safe resume from last collected candle after restart
- Idempotent INSERT IGNORE for duplicate prevention

## Architecture

```
main.py                 # Service orchestrator and main loop
├── config.py          # Configuration management
├── mt5_connector.py   # MT5 connection with reconnect logic
├── data_fetcher.py    # Historical and live data fetching
├── database.py        # MySQL storage with idempotent writes
└── logger.py          # Logging to console and database
```

## Database Schema

### `candles` Table
Stores OHLCV market data:
- `id`: Auto-incrementing primary key
- `symbol`: Trading instrument (e.g., US30, USTech)
- `timeframe`: Candle timeframe (e.g., M1, H1, D1)
- `timestamp`: UTC timestamp of candle open
- `open`, `high`, `low`, `close`: Price data (real)
- `volume`: Tick volume (integer)
- `created_at`, `updated_at`: Record timestamps

**Indexes:**
- Unique composite index on (symbol, timeframe, timestamp)
- Index on timestamp for time-range queries
- Index on (symbol, timeframe) for instrument queries

**Note:** Tables and indexes are automatically created when the service starts for the first time.

### `data_collection_logs` Table
Tracks service operations:
- `id`: Auto-incrementing primary key
- `timestamp`: Log timestamp
- `level`: Log level (INFO, WARNING, ERROR)
- `symbol`, `timeframe`: Context if applicable
- `message`: Log message
- `details`: Additional data as JSON

## Setup Instructions

### Prerequisites
1. **MetaTrader 5** terminal installed and configured
2. **MySQL Server** (e.g. via MySQL Workbench) with a database `trading_app_1` (or as set in `.env`)
3. **Python 3.8+** installed

### Installation

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment variables:**

   Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```

   Edit `.env` with your credentials:
   ```env
   # MT5 Account Credentials
   MT5_LOGIN=your_account_number
   MT5_PASSWORD=your_password
   MT5_SERVER=your_broker_server

   # MySQL Database Connection (local MySQL Server / MySQL Workbench)
   MYSQL_HOST=localhost
   MYSQL_PORT=3306
   MYSQL_USER=root
   MYSQL_PASSWORD=your_mysql_password_here
   MYSQL_DATABASE=trading_app_1
   ```

3. **Set up MySQL:**

   - Install MySQL Server (or use MySQL Workbench with a local server).
   - Create a database named `trading_app_1` (or set `MYSQL_DATABASE` in `.env`).
   - Ensure the user in `MYSQL_USER` has privileges on that database.
   - Set `MYSQL_PASSWORD` in `.env` (use an environment variable; do not hardcode).

4. **Verify MT5 Terminal:**
   - Ensure MT5 terminal is running
   - Login to your account manually at least once
   - Enable automated trading in MT5 settings

### Configuration

Edit `config.py` to customize:

```python
# Symbols to collect
SYMBOLS = ['US30', 'USTech']

# Timeframes to collect (add/remove as needed)
TIMEFRAMES = {
    'M1': 1,    # 1 minute
    'M5': 5,    # 5 minutes
    'M15': 15,  # 15 minutes
    'M30': 30,  # 30 minutes
    'H1': 60,   # 1 hour
    'H4': 240,  # 4 hours
    'D1': 1440, # 1 day
}

# Collection interval (seconds between updates)
COLLECTION_INTERVAL_SECONDS = 60

# Historical data to fetch on first run (days)
HISTORICAL_DAYS_LOOKBACK = 365

# Reconnection settings
MAX_RECONNECT_ATTEMPTS = 5
RECONNECT_DELAY_SECONDS = 10
```

## Running the Service

### Start the Service
```bash
python main.py
```

### Expected Output
```
Initializing MT5 Market Data Collector...
MySQL database connected
Connected to MT5 account...

============================================================
MT5 Market Data Collector
============================================================
Symbols: US30, USTech
Timeframes: M1, M5, M15, M30, H1, H4, D1
Collection Interval: 60s
============================================================

Starting initial historical data sync...
Fetched 50000 candles for US30 M1
...
Initial sync completed

Starting live data collection...
Press Ctrl+C to stop
```

### Stopping the Service
Press `Ctrl+C` to stop gracefully. The service will:
- Close MT5 connection
- Close database connection
- Log shutdown event

## Service Behavior

### On First Start
1. Connects to MT5 and database
2. Performs initial historical sync for all symbols/timeframes
3. Detects and fills any gaps in historical data
4. Begins live data collection loop

### During Normal Operation
1. Every 60 seconds (configurable):
   - Fetches latest candles for all symbols/timeframes
   - Inserts new candles (duplicates ignored)
2. Every 10 cycles:
   - Runs gap detection and backfill

### On Restart
1. Resumes from last stored candle for each symbol/timeframe
2. Fills any gaps that occurred during downtime
3. Continues live collection

### Connection Failures
- **MT5 disconnect**: Attempts reconnection up to 5 times with 10s delay
- **Database disconnect**: Automatically reconnects on next operation
- All errors logged to database and console

## Data Access

### Query Examples

**Get latest 100 candles for US30 H1:**
```sql
SELECT timestamp, open, high, low, close, volume
FROM candles
WHERE symbol = 'US30' AND timeframe = 'H1'
ORDER BY timestamp DESC
LIMIT 100;
```

**Get candles in specific time range:**
```sql
SELECT *
FROM candles
WHERE symbol = 'USTech'
  AND timeframe = 'D1'
  AND timestamp >= '2024-01-01'
  AND timestamp < '2024-02-01'
ORDER BY timestamp ASC;
```

**Check data completeness:**
```sql
SELECT symbol, timeframe,
       MIN(timestamp) as first_candle,
       MAX(timestamp) as last_candle,
       COUNT(*) as total_candles
FROM candles
GROUP BY symbol, timeframe;
```

**View collection logs:**
```sql
SELECT timestamp, level, symbol, timeframe, message
FROM data_collection_logs
WHERE level = 'ERROR'
ORDER BY timestamp DESC
LIMIT 50;
```

## Monitoring

### Check Service Health
Monitor the `data_collection_logs` table for:
- Connection failures
- Gap detection events
- Data collection errors

### Key Metrics
- Candle count per symbol/timeframe
- Last collected timestamp
- Error frequency and types
- Gap occurrences

### Log Levels
- **INFO**: Normal operations (connections, data fetched)
- **WARNING**: Non-critical issues (no data available, gaps detected)
- **ERROR**: Critical failures (connection lost, insert failed)

## Troubleshooting

### MT5 Connection Fails
- Verify MT5 terminal is running
- Check credentials in `.env`
- Ensure automated trading is enabled in MT5
- Verify firewall allows MT5 connections

### No Data Collected
- Check if symbols are available in your MT5 account
- Verify symbol names match exactly (case-sensitive)
- Check MT5 market watch for symbol visibility

### Database Errors
- Verify MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, and MYSQL_DATABASE in `.env`
- Ensure MySQL Server is running and the database exists
- Tables are created automatically on first run
- Ensure the MySQL user has CREATE and INSERT privileges

### Gaps in Data
- Normal during market closures (weekends, holidays)
- Service will backfill gaps automatically
- Check logs for collection failures during gap periods

## Security Notes

- ⚠️ Never commit `.env` file to source control
- Store credentials securely
- Use read-only MT5 accounts if possible
- Keep MYSQL_PASSWORD in `.env` only; never hardcode or commit it
- Use a dedicated MySQL user with minimal required privileges

## Performance

### Resource Usage
- CPU: Minimal (mostly idle, spikes during collection)
- RAM: ~50-100MB depending on data volume
- Disk: Depends on data retention
- Network: Low bandwidth, periodic MT5 API calls

### Data Volume Estimates
Per symbol per timeframe per year:
- M1: ~365,000 candles (~50MB)
- M5: ~73,000 candles (~10MB)
- H1: ~6,000 candles (~1MB)
- D1: ~250 candles (~50KB)

## Extension Points

### Adding New Symbols
Edit `config.py`:
```python
SYMBOLS = ['US30', 'USTech', 'EURUSD', 'GBPUSD']
```

### Adding New Timeframes
Edit `config.py`:
```python
TIMEFRAMES = {
    'M1': 1,
    'M5': 5,
    'W1': 10080,  # Weekly
}
```

Update `data_fetcher.py` TIMEFRAME_MAP:
```python
TIMEFRAME_MAP = {
    'W1': mt5.TIMEFRAME_W1,
}
```

### Custom Data Processing
Extend `data_fetcher.py` to add:
- Custom indicators
- Data validation rules
- Additional calculated fields

## Support

For issues related to:
- **MT5 Python API**: [MetaQuotes Documentation](https://www.mql5.com/en/docs/python_metatrader5)
- **MySQL**: [MySQL Documentation](https://dev.mysql.com/doc/)
- **mysql-connector-python**: [MySQL Connector/Python](https://dev.mysql.com/doc/connector-python/en/)
- **Service Logic**: Review logs in `data_collection_logs` table

## License

This is a proprietary data collection service for internal use.
