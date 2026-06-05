INSTALL ducklake;
INSTALL postgres;

LOAD ducklake;
LOAD postgres;


CREATE OR REPLACE SECRET ducklake_secret (
  TYPE ducklake,
  METADATA_PATH '',
  DATA_PATH '/opt/gizmosql/data/ducklake_files',
  METADATA_PARAMETERS MAP {
    'TYPE': 'postgres'
  }
);

ATTACH 'ducklake:ducklake_secret' AS ducklake;

USE ducklake;

CREATE SCHEMA IF NOT EXISTS ducklake.bafrapy;

CREATE TABLE IF NOT EXISTS ducklake.bafrapy.crypto_ohlcv (
    time TIMESTAMPTZ NOT NULL,

    exchange VARCHAR NOT NULL,
    symbol VARCHAR NOT NULL,
    resolution UINTEGER NOT NULL,

    open DECIMAL(38,0) NOT NULL,
    high DECIMAL(38,0) NOT NULL,
    low DECIMAL(38,0) NOT NULL,
    close DECIMAL(38,0) NOT NULL,
    volume DECIMAL(38,0) NOT NULL,
    quote_volume DECIMAL(38,0) NOT NULL,

    base_decimals UTINYINT NOT NULL,
    quote_decimals UTINYINT NOT NULL,

    generated BOOLEAN NOT NULL DEFAULT FALSE
);

ALTER TABLE ducklake.bafrapy.crypto_ohlcv
SET PARTITIONED BY (
    exchange,
    symbol,
    resolution,
    year(time)
);