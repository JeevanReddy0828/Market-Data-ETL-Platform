CREATE TABLE IF NOT EXISTS dim_security (
  security_id SERIAL PRIMARY KEY,
  symbol TEXT UNIQUE NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_prices_daily (
  symbol TEXT NOT NULL,
  trading_date DATE NOT NULL,
  open NUMERIC,
  high NUMERIC,
  low NUMERIC,
  close NUMERIC,
  volume BIGINT,
  source TEXT NOT NULL DEFAULT 'stooq',
  ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY(symbol, trading_date)
);

CREATE TABLE IF NOT EXISTS fact_returns_daily (
  symbol TEXT NOT NULL,
  trading_date DATE NOT NULL,
  daily_return NUMERIC,
  PRIMARY KEY(symbol, trading_date)
);

CREATE TABLE IF NOT EXISTS fact_volatility_30d (
  symbol TEXT NOT NULL,
  trading_date DATE NOT NULL,
  vol_30d NUMERIC,
  PRIMARY KEY(symbol, trading_date)
);

CREATE TABLE IF NOT EXISTS etl_run_audit (
  run_id TEXT PRIMARY KEY,
  started_at TIMESTAMP NOT NULL,
  finished_at TIMESTAMP,
  status TEXT NOT NULL,
  symbols INT NOT NULL DEFAULT 0,
  extracted_rows BIGINT NOT NULL DEFAULT 0,
  loaded_prices BIGINT NOT NULL DEFAULT 0,
  dq_null_violations BIGINT NOT NULL DEFAULT 0,
  dq_duplicate_violations BIGINT NOT NULL DEFAULT 0,
  dq_nonpositive_price BIGINT NOT NULL DEFAULT 0,
  message TEXT
);

CREATE INDEX IF NOT EXISTS idx_prices_date ON fact_prices_daily(trading_date);
CREATE INDEX IF NOT EXISTS idx_returns_date ON fact_returns_daily(trading_date);
