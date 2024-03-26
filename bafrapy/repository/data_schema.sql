
CREATE TABLE IF NOT EXISTS crypto_ohlcv (
    time Datetime NOT NULL,
    symbol String NOT NULL,
    resolution UInt64 NOT NULL,
    open Decimal(18,8) NOT NULL,
    high Decimal(18,8) NOT NULL,
    low Decimal(18,8) NOT NULL,
    close Decimal(18,8) NOT NULL,
    volume Decimal(18,8) NOT NULL,
    state Enum8('ORIGINAL', 'GAP') NOT NULL,
)
ENGINE = ReplacingMergeTree()
PARTITION BY (symbol)
ORDER BY (time, symbol, resolution)
PRIMARY KEY (time, symbol, resolution);
