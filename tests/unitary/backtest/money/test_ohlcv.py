from datetime import datetime
from decimal import Decimal

import pytest

from bafrapy.backtest.money import Currency, EMoney, OHLCV, Pair


class TestOHLCV:
    def make_ohlcv(self, **overrides) -> OHLCV:
        data = {
            "pair": Pair(base=Currency("BTC"), quote=Currency("USD")),
            "resolution": 86400,
            "base_decimals": 8,
            "quote_decimals": 2,
            "timestamp": datetime(2026, 1, 1, 0, 0, 0),
            "open": 100 * 10**2,
            "high": 120 * 10**2,
            "low": 90 * 10**2,
            "close": 110 * 10**2,
            "volume": 1000 * 10**8,
            "quote_volume": 2000 * 10**2,
        }
        data.update(overrides)
        return OHLCV(**data)

    def test_create_ohlcv(self):
        ohlcv = self.make_ohlcv()

        assert ohlcv.pair == Pair(base=Currency("BTC"), quote=Currency("USD"))
        assert ohlcv.resolution == 86400
        assert ohlcv.base_decimals == 8
        assert ohlcv.quote_decimals == 2
        assert ohlcv.timestamp == datetime(2026, 1, 1, 0, 0, 0)
        assert ohlcv.open == 100 * 10**2
        assert ohlcv.high == 120 * 10**2
        assert ohlcv.low == 90 * 10**2
        assert ohlcv.close == 110 * 10**2
        assert ohlcv.volume == 1000 * 10**8
        assert ohlcv.quote_volume == 2000 * 10**2
        assert ohlcv.open_emoney == EMoney(value=100 * 10**2, currency=Currency("USD"), decimals=2)
        assert ohlcv.high_emoney == EMoney(value=120 * 10**2, currency=Currency("USD"), decimals=2)
        assert ohlcv.low_emoney == EMoney(value=90 * 10**2, currency=Currency("USD"), decimals=2)
        assert ohlcv.close_emoney == EMoney(value=110 * 10**2, currency=Currency("USD"), decimals=2)
        assert ohlcv.volume_emoney == EMoney(value=1000 * 10**8, currency=Currency("BTC"), decimals=8)
        assert ohlcv.quote_volume_emoney == EMoney(value=2000 * 10**2, currency=Currency("USD"), decimals=2)

    def test_create_ohlcv_from_decimal(self):
        ohlcv = OHLCV.from_decimal(
            pair=Pair(base=Currency("BTC"), quote=Currency("USD")),
            resolution=86400,
            timestamp=datetime(2026, 1, 1, 0, 0, 0),
            open=Decimal("100.00"),
            high=Decimal("120.00"),
            low=Decimal("90.00"),
            close=Decimal("110.00"),
            volume=Decimal("1000.00"),
            quote_volume=Decimal("2000.00"),
        )
        assert ohlcv.base_decimals == 2
        assert ohlcv.quote_decimals == 2
        assert ohlcv.open == 10000
        assert ohlcv.high == 12000
        assert ohlcv.low == 9000
        assert ohlcv.close == 11000
        assert ohlcv.volume == 100000
        assert ohlcv.quote_volume == 200000
        assert ohlcv.open_emoney == EMoney(value=10000, currency=Currency("USD"), decimals=2)
        assert ohlcv.high_emoney == EMoney(value=12000, currency=Currency("USD"), decimals=2)
        assert ohlcv.low_emoney == EMoney(value=9000, currency=Currency("USD"), decimals=2)
        assert ohlcv.close_emoney == EMoney(value=11000, currency=Currency("USD"), decimals=2)
        assert ohlcv.volume_emoney == EMoney(value=100000, currency=Currency("BTC"), decimals=2)
        assert ohlcv.quote_volume_emoney == EMoney(value=200000, currency=Currency("USD"), decimals=2)

    def test_create_ohlcv_from_decimal_rejects_mixed_quote_decimals(self):
        with pytest.raises(ValueError):
            OHLCV.from_decimal(
                pair=Pair(base=Currency("BTC"), quote=Currency("USD")),
                resolution=86400,
                timestamp=datetime(2026, 1, 1, 0, 0, 0),
                open=Decimal("100.00"),
                high=Decimal("120.000"),
                low=Decimal("90.00"),
                close=Decimal("110.00"),
                volume=Decimal("1000.00"),
                quote_volume=Decimal("2000.00"),
            )

    def test_create_ohlcv_from_decimal_rejects_mixed_quote_volume_decimals(self):
        with pytest.raises(ValueError):
            OHLCV.from_decimal(
                pair=Pair(base=Currency("BTC"), quote=Currency("USD")),
                resolution=86400,
                timestamp=datetime(2026, 1, 1, 0, 0, 0),
                open=Decimal("100.00"),
                high=Decimal("120.00"),
                low=Decimal("90.00"),
                close=Decimal("110.00"),
                volume=Decimal("1000.00"),
                quote_volume=Decimal("2000.000"),
            )

    def test_create_ohlcv_from_float(self):
        ohlcv = OHLCV.from_float(
            pair=Pair(base=Currency("BTC"), quote=Currency("USD")),
            resolution=86400,
            timestamp=datetime(2026, 1, 1, 0, 0, 0),
            open=100.1,
            high=120.1,
            low=90.1,
            close=110.1,
            volume=1000.1,
            quote_volume=2000.1,
        )
        assert ohlcv.base_decimals == 1
        assert ohlcv.quote_decimals == 1
        assert ohlcv.open == 1001
        assert ohlcv.quote_volume == 20001
        assert ohlcv.open_emoney == EMoney(value=1001, currency=Currency("USD"), decimals=1)
        assert ohlcv.quote_volume_emoney == EMoney(value=20001, currency=Currency("USD"), decimals=1)

    @pytest.mark.parametrize(
        ("field_name", "value"),
        [
            ("pair", "not-a-pair"),
            ("resolution", "-1"),
            ("base_decimals", "8"),
            ("quote_decimals", "2"),
            ("timestamp", "not-a-datetime"),
            ("open", "100"),
            ("high", "120"),
            ("low", "90"),
            ("close", "110"),
            ("volume", "1000"),
            ("quote_volume", "2000"),
        ],
    )
    def test_invalid_type_per_field(self, field_name, value):
        with pytest.raises((TypeError, ValueError)):
            self.make_ohlcv(**{field_name: value})

    @pytest.mark.parametrize(
        ("field_name", "value"),
        [
            ("base_decimals", -1),
            ("quote_decimals", -1),
            ("high", -1),
            ("low", -1),
            ("close", -1),
            ("volume", -1),
        ],
    )
    def test_negative_values_are_rejected(self, field_name, value):
        with pytest.raises(ValueError):
            self.make_ohlcv(**{field_name: value})

    def test_high_cannot_be_lower_than_low(self):
        with pytest.raises(ValueError):
            self.make_ohlcv(high=80, low=90)

    def test_close_cannot_be_lower_than_low(self):
        with pytest.raises(ValueError):
            self.make_ohlcv(low=90, close=80)

    def test_close_cannot_be_greater_than_high(self):
        with pytest.raises(ValueError):
            self.make_ohlcv(high=100, close=110)
