/*
  # MT5 Market Data Collection Schema

  1. New Tables
    - `candles`
      - `id` (bigint, primary key, auto-increment)
      - `symbol` (text, not null) - Trading instrument (US30, USTech)
      - `timeframe` (text, not null) - Candle timeframe (M1, M5, M15, H1, H4, D1, etc.)
      - `timestamp` (timestamptz, not null) - UTC timestamp of candle open
      - `open` (numeric, not null) - Opening price
      - `high` (numeric, not null) - Highest price
      - `low` (numeric, not null) - Lowest price
      - `close` (numeric, not null) - Closing price
      - `volume` (bigint, not null) - Volume
      - `created_at` (timestamptz) - Record creation timestamp
      - `updated_at` (timestamptz) - Record update timestamp

    - `data_collection_logs`
      - `id` (bigint, primary key, auto-increment)
      - `timestamp` (timestamptz, not null) - Log timestamp
      - `level` (text, not null) - Log level (INFO, WARNING, ERROR)
      - `symbol` (text) - Related symbol if applicable
      - `timeframe` (text) - Related timeframe if applicable
      - `message` (text, not null) - Log message
      - `details` (jsonb) - Additional details

  2. Indexes
    - Composite unique index on (symbol, timeframe, timestamp) for idempotent writes
    - Index on timestamp for fast time-range queries
    - Index on (symbol, timeframe) for instrument-specific queries
    - Index on data_collection_logs timestamp for log retrieval

  3. Notes
    - Using numeric type for prices to avoid floating-point precision issues
    - Timestamps are stored in UTC with timezone awareness
    - Unique constraint ensures no duplicate candles
    - Logs table tracks collection process for debugging and monitoring
*/

-- Create candles table
CREATE TABLE IF NOT EXISTS candles (
  id bigserial PRIMARY KEY,
  symbol text NOT NULL,
  timeframe text NOT NULL,
  timestamp timestamptz NOT NULL,
  open numeric NOT NULL,
  high numeric NOT NULL,
  low numeric NOT NULL,
  close numeric NOT NULL,
  volume bigint NOT NULL,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- Create unique constraint to prevent duplicate candles
CREATE UNIQUE INDEX IF NOT EXISTS candles_symbol_timeframe_timestamp_idx 
  ON candles(symbol, timeframe, timestamp);

-- Create indexes for fast queries
CREATE INDEX IF NOT EXISTS candles_timestamp_idx ON candles(timestamp);
CREATE INDEX IF NOT EXISTS candles_symbol_timeframe_idx ON candles(symbol, timeframe);

-- Create data collection logs table
CREATE TABLE IF NOT EXISTS data_collection_logs (
  id bigserial PRIMARY KEY,
  timestamp timestamptz NOT NULL DEFAULT now(),
  level text NOT NULL,
  symbol text,
  timeframe text,
  message text NOT NULL,
  details jsonb
);

-- Create index for log queries
CREATE INDEX IF NOT EXISTS logs_timestamp_idx ON data_collection_logs(timestamp);
CREATE INDEX IF NOT EXISTS logs_level_idx ON data_collection_logs(level);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to automatically update updated_at
DROP TRIGGER IF EXISTS update_candles_updated_at ON candles;
CREATE TRIGGER update_candles_updated_at
  BEFORE UPDATE ON candles
  FOR EACH ROW
  EXECUTE FUNCTION update_updated_at_column();
