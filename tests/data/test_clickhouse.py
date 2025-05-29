from datetime import datetime, timedelta
from decimal import Decimal

import pandas as pd
import pytest

from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client
from testcontainers.clickhouse import ClickHouseContainer

from bafrapy.datawarehouse.repository import ClikhouseOHLCVRepository
from bafrapy.logger.log import LoguruLogger as log


single_integer = {
    "provider": "test_provider",
    "symbol": "test_symbol",
    "resolution": 86400,
    "time": datetime(2024, 1, 1),
    "open": 100, 
    "high": 110, 
    "low": 90, 
    "close": 105, 
    "volume": 1000
}

single_decimal = {
    "provider": "test_provider",
    "symbol": "test_symbol",
    "resolution": 86400,
    "time": datetime(2024, 1, 1),
    "open": Decimal("100.00"),
    "high": Decimal("110.00"),
    "low": Decimal("90.00"),
    "close": Decimal("105.00"),
    "volume": Decimal("1000.00")
}

multiple_decimal = [
    {
        "provider": "test_provider",
        "symbol": "test_symbol",
        "resolution": 1,
        "time": datetime(2024, 1, 1),
        "open": Decimal("100.00"),
        "high": Decimal("105.00"),
        "low": Decimal("95.00"),
        "close": Decimal("105.00"),
        "volume": Decimal("100.00")
    },
    {
        "provider": "test_provider",
        "symbol": "test_symbol",
        "resolution": 1,
        "time": datetime(2024, 1, 2),
        "open": Decimal("105.00"),
        "high": Decimal("110.00"),
        "low": Decimal("90.00"),
        "close": Decimal("110.00"),
        "volume": Decimal("200.00")
    },
    {
        "provider": "test_provider",
        "symbol": "test_symbol",
        "resolution": 1,
        "time": datetime(2024, 1, 3),
        "open": Decimal("110.00"),
        "high": Decimal("115.00"),
        "low": Decimal("85.00"),
        "close": Decimal("115.00"),
        "volume": Decimal("300.00")
    },
    {
        "provider": "test_provider",
        "symbol": "test_symbol",
        "resolution": 1,
        "time": datetime(2024, 1, 4),
        "open": Decimal("115.00"),
        "high": Decimal("120.00"),
        "low": Decimal("80.00"),
        "close": Decimal("120.00"),
        "volume": Decimal("400.00")
    },
    {
        "provider": "test_provider",
        "symbol": "test_symbol",
        "resolution": 1,
        "time": datetime(2024, 1, 5),
        "open": Decimal("120.00"),
        "high": Decimal("125.00"),
        "low": Decimal("75.00"),
        "close": Decimal("125.00"),
        "volume": Decimal("500.00")
    }
]

multiple_providers = [
    {
        "provider": "test_provider-1",
        "symbol": "test_symbol",
        "resolution": 86400,
        "time": datetime(2024, 1, 1),
        "open": Decimal("100.00"),
        "high": Decimal("105.00"),
        "low": Decimal("95.00"),
        "close": Decimal("105.00"),
        "volume": Decimal("100.00")
    },
    {
        "provider": "test_provider-1",
        "symbol": "test_symbol",
        "resolution": 86400,
        "time": datetime(2024, 1, 2),
        "open": Decimal("105.00"),
        "high": Decimal("110.00"),
        "low": Decimal("90.00"),
        "close": Decimal("110.00"),
        "volume": Decimal("200.00")
    },
    {
        "provider": "test_provider-2",
        "symbol": "test_symbol",
        "resolution": 86400,
        "time": datetime(2024, 1, 1),
        "open": Decimal("110.00"),
        "high": Decimal("115.00"),
        "low": Decimal("85.00"),
        "close": Decimal("115.00"),
        "volume": Decimal("300.00")
    },
    {
        "provider": "test_provider-2",
        "symbol": "test_symbol",
        "resolution": 86400,
        "time": datetime(2024, 1, 2),
        "open": Decimal("115.00"),
        "high": Decimal("120.00"),
        "low": Decimal("80.00"),
        "close": Decimal("120.00"),
        "volume": Decimal("400.00")
    }
]

multiple_symbols = [
    {
        "provider": "test_provider",
        "symbol": "test_symbol-1",
        "resolution": 86400,
        "time": datetime(2024, 1, 1),
        "open": Decimal("100.00"),
        "high": Decimal("105.00"),
        "low": Decimal("95.00"),
        "close": Decimal("105.00"),
        "volume": Decimal("100.00")
    },
    {
        "provider": "test_provider",
        "symbol": "test_symbol-1",
        "resolution": 86400,
        "time": datetime(2024, 1, 2),
        "open": Decimal("105.00"),
        "high": Decimal("110.00"),
        "low": Decimal("90.00"),
        "close": Decimal("110.00"),
        "volume": Decimal("200.00")
    },
    {
        "provider": "test_provider",
        "symbol": "test_symbol-2",
        "resolution": 86400,
        "time": datetime(2024, 1, 1),
        "open": Decimal("110.00"),
        "high": Decimal("115.00"),
        "low": Decimal("85.00"),
        "close": Decimal("115.00"),
        "volume": Decimal("300.00")
    },
    {
        "provider": "test_provider",
        "symbol": "test_symbol-2",
        "resolution": 86400,
        "time": datetime(2024, 1, 2),
        "open": Decimal("115.00"),
        "high": Decimal("120.00"),
        "low": Decimal("80.00"),
        "close": Decimal("120.00"),
        "volume": Decimal("400.00")
    }
]


gap_data = [
    {
        "provider": "test_provider",
        "symbol": "test_symbol",
        "resolution": 86400,
        "time": datetime(2024, 1, 1),
        "open": Decimal("100.00"),
        "high": Decimal("105.00"),
        "low": Decimal("95.00"),
        "close": Decimal("105.00"),
        "volume": Decimal("100.00")
    },
    {
        "provider": "test_provider",
        "symbol": "test_symbol",
        "resolution": 86400,
        "time": datetime(2024, 1, 5),
        "open": Decimal("120.00"),
        "high": Decimal("125.00"),
        "low": Decimal("75.00"),
        "close": Decimal("125.00"),
        "volume": Decimal("500.00")
    }
]


class TestOHLCVRepository:
    repo: ClikhouseOHLCVRepository
    client: Client

    def get_repo(self):
        return self.__class__.repo
    
    def get_client(self):
        return self.__class__.client

    @pytest.fixture(scope="class", autouse=True)
    def setup_class(self):
        log().deactivate()
        container = ClickHouseContainer("clickhouse/clickhouse-server:23.9.5-alpine", 8123, "test", "test", "bafrapy")
        container.with_exposed_ports(8123)
        container.with_bind_ports(8123, 8129)
        container.start()
        client = get_client(host=container.get_container_host_ip(), 
                            username="test", password="test", 
                            port=int(container.get_exposed_port(8123)),
                            database="bafrapy")
        repo = ClikhouseOHLCVRepository(client)
        repo.initialize()
        self.__class__.repo = repo
        self.__class__.client = client
        yield

        container.stop()

    @pytest.fixture(autouse=True)
    def truncate_table(self):
        yield
        self.get_client().command("TRUNCATE TABLE bafrapy.crypto_ohlcv")

    def query_df(self, provider, symbol, resolution, start, end):
        return self.get_client().query_df(
            '''SELECT * FROM crypto_ohlcv 
                WHERE provider={provider:String} 
                    AND symbol={symbol:String} 
                    AND resolution={resolution:Int} 
                    AND time BETWEEN {start:Date} AND {end:Date}
            ORDER BY time''',
            parameters={
                "provider": provider,
                "symbol": symbol,
                "resolution": resolution,
                "start": start,
                "end": end
            }
        )
    
    def test_insert_empty_df(self):
        df = pd.DataFrame()
        self.repo.insert_data(df)
        count = self.repo.count_rows()
        assert count == 0

    def test_insert_single_integer_candle(self):
        df = pd.DataFrame([single_integer])
        assert len(df) == 1
        self.repo.insert_data(df)
        result = self.query_df(
            single_integer["provider"],
            single_integer["symbol"],
            single_integer["resolution"],
            single_integer["time"].date(),
            single_integer["time"].date()
        )
        assert len(result) == 1
        assert result.iloc[0]["provider"] == single_integer["provider"]
        assert result.iloc[0]["symbol"] == single_integer["symbol"]
        assert result.iloc[0]["resolution"] == single_integer["resolution"]
        assert result.iloc[0]["time"] == single_integer["time"]
        assert result.iloc[0]["open"] == single_integer["open"]
        assert result.iloc[0]["high"] == single_integer["high"]
        assert result.iloc[0]["low"] == single_integer["low"]
        assert result.iloc[0]["close"] == single_integer["close"]
        assert result.iloc[0]["volume"] == single_integer["volume"]

    def test_insert_single_decimal_candle(self):
        df = pd.DataFrame([single_decimal])
        assert len(df) == 1
        self.repo.insert_data(df)
        result = self.query_df(
            single_decimal["provider"],
            single_decimal["symbol"],
            single_decimal["resolution"],
            single_decimal["time"].date(),
            single_decimal["time"].date()
        )
        assert len(result) == 1
        assert result.iloc[0]["provider"] == single_decimal["provider"]
        assert result.iloc[0]["symbol"] == single_decimal["symbol"]
        assert result.iloc[0]["resolution"] == single_decimal["resolution"]
        assert result.iloc[0]["time"] == single_decimal["time"]
        assert result.iloc[0]["open"] == single_decimal["open"]
        assert result.iloc[0]["high"] == single_decimal["high"]
        assert result.iloc[0]["low"] == single_decimal["low"]
        assert result.iloc[0]["close"] == single_decimal["close"]
        assert result.iloc[0]["volume"] == single_decimal["volume"]

    def test_insert_batch_candles(self):
        df = pd.DataFrame(multiple_decimal)
        self.repo.insert_data(df)

        result = self.query_df(
            multiple_decimal[0]["provider"],
            multiple_decimal[0]["symbol"],
            multiple_decimal[0]["resolution"],
            multiple_decimal[0]["time"].date(),
            multiple_decimal[-1]["time"].date()
        )
        assert len(result) == len(multiple_decimal)

        for i in range(len(multiple_decimal)):
            row = result.iloc[i]
            assert row["provider"] == multiple_decimal[i]["provider"]
            assert row["symbol"] == multiple_decimal[i]["symbol"]
            assert row["resolution"] == multiple_decimal[i]["resolution"]
            assert row["time"] == multiple_decimal[i]["time"]
            assert row["open"] == multiple_decimal[i]["open"]
            assert row["high"] == multiple_decimal[i]["high"]
            assert row["low"] == multiple_decimal[i]["low"]
            assert row["close"] == multiple_decimal[i]["close"]
            assert row["volume"] == multiple_decimal[i]["volume"]

    def test_insert_fill_gaps(self):
        df = pd.DataFrame(gap_data)
        self.repo.insert_data(df, fill_gaps=True)
        result = self.query_df(
            gap_data[0]["provider"],
            gap_data[0]["symbol"],
            gap_data[0]["resolution"],
            gap_data[0]["time"].date(),
            gap_data[-1]["time"].date()
        )

        assert len(result) == 5
        assert result.iloc[0]["provider"] == gap_data[0]["provider"]
        assert result.iloc[0]["symbol"] == gap_data[0]["symbol"]
        assert result.iloc[0]["resolution"] == gap_data[0]["resolution"]
        assert result.iloc[0]["time"] == gap_data[0]["time"]
        assert result.iloc[0]["open"] == gap_data[0]["open"]
        assert result.iloc[0]["high"] == gap_data[0]["high"]
        assert result.iloc[0]["low"] == gap_data[0]["low"]
        assert result.iloc[0]["close"] == gap_data[0]["close"]
        assert result.iloc[0]["volume"] == gap_data[0]["volume"]

        close = gap_data[0]["close"]
        for i in range(1, 4):
            assert result.iloc[i]["provider"] == gap_data[0]["provider"]
            assert result.iloc[i]["symbol"] == gap_data[0]["symbol"]
            assert result.iloc[i]["resolution"] == gap_data[0]["resolution"]
            assert result.iloc[i]["time"] == gap_data[0]["time"] + timedelta(days=i)
            assert result.iloc[i]["open"] == close
            assert result.iloc[i]["high"] == close
            assert result.iloc[i]["low"] == close
            assert result.iloc[i]["close"] == close
            assert result.iloc[i]["volume"] == 0
        
        assert result.iloc[4]["provider"] == gap_data[1]["provider"]
        assert result.iloc[4]["symbol"] == gap_data[1]["symbol"]
        assert result.iloc[4]["resolution"] == gap_data[1]["resolution"]
        assert result.iloc[4]["time"] == gap_data[1]["time"]
        assert result.iloc[4]["open"] == gap_data[1]["open"]
        assert result.iloc[4]["high"] == gap_data[1]["high"]
        assert result.iloc[4]["low"] == gap_data[1]["low"]
        assert result.iloc[4]["close"] == gap_data[1]["close"]
        assert result.iloc[4]["volume"] == gap_data[1]["volume"]

    def test_insert_batch_distinct_candles(self):
        provider = "test_provider"
        symbol = "test_symbol"
        resolution = 86400
        start_date = datetime(2024, 1, 1)
        days = 5
        base = Decimal("100.00")
        delta = Decimal("5.00")
        base_volume = Decimal("100.00")
        volume_delta = Decimal("100.00")

        rows = []
        for i in range(5):
            d = start_date + timedelta(days=i)
            rows.append({
                "provider": provider,
                "symbol": symbol,
                "resolution": resolution,
                "time": d,
                "open": base + delta * i,                  # 100, 105, 110, 115, 120
                "high": base + delta * (i+1),              # 105, 110, 115, 120, 125
                "low": base - delta * i,                   #  95,  90,  85,  80,  75
                "close": base + delta * (i+1),             # 105, 110, 115, 120, 125
                "volume": base_volume + volume_delta * i   # 100, 200, 300, 400, 500
            })

        df = pd.DataFrame(rows)
        assert len(df) == 5

        self.repo.insert_data(df)

        result = self.query_df(provider, symbol, resolution, start_date.date(), start_date.date() + timedelta(days=days - 1))
        assert len(result) == 5

        result = result.sort_values("time").reset_index(drop=True)

        for i in range(5):
            expected_date = start_date + timedelta(days=i)
            row = result.iloc[i]
            assert row["provider"] == provider
            assert row["symbol"] == symbol
            assert row["resolution"] == resolution
            assert row["time"] == expected_date
            assert row["open"] == base + delta * i
            assert row["high"] == base + delta * (i+1)
            assert row["low"] == base - delta * i
            assert row["close"] == base + delta * (i+1)
            assert row["volume"] == base_volume + volume_delta * i
