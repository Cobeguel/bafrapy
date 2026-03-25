import pytest

from datetime import datetime
from decimal import Decimal

from bafrapy.backtest.money import OHLCV, Pair, Currency, EMoney


class TestOHLCV:
    def make_ohlcv(self, **overrides) -> OHLCV:
        data = {
            "pair": Pair(base=Currency("BTC", 8), quote=Currency("USD", 2)),
            "resolution": 86400,
            "timestamp": datetime(2026, 1, 1, 0, 0, 0),
            "open": 100**8,
            "high": 120**8,
            "low": 90**8,
            "close": 110**8,
            "volume": 1000**8,
            "quote_volume": 2000**2,
        }
        data.update(overrides)
        return OHLCV(**data)

    def test_create_ohlcv(self):
        ohlcv = self.make_ohlcv()

        assert ohlcv.pair == Pair(base=Currency("BTC", 8), quote=Currency("USD", 2))
        assert ohlcv.resolution == 86400
        assert ohlcv.timestamp == datetime(2026, 1, 1, 0, 0, 0)
        assert ohlcv.open == EMoney(value=100, currency=Currency("BTC", 8))
        assert ohlcv.high == EMoney(value=120, currency=Currency("BTC", 8))
        assert ohlcv.low == EMoney(value=90, currency=Currency("BTC", 8))
        assert ohlcv.close == EMoney(value=110, currency=Currency("BTC", 8))
        assert ohlcv.volume == EMoney(value=1000, currency=Currency("BTC", 8))
        assert ohlcv.quote_volume == EMoney(value=2000, currency=Currency("USD", 2))

    def test_create_ohlcv_from_decimal(self):
        ohlcv = OHLCV.from_decimal(
            pair=Pair(base=Currency("BTC", 8), quote=Currency("USD", 2)),
            resolution=86400,
            timestamp=datetime(2026, 1, 1, 0, 0, 0),
            open=Decimal("100.00"),
            high=Decimal("120.00"),
            low=Decimal("90.00"),
            close=Decimal("110.00"),
            volume=Decimal("1000.00"),
            quote_volume=Decimal("2000.00"),
        )
        assert ohlcv.pair == Pair(base=Currency("BTC", 8), quote=Currency("USD", 2))
        assert ohlcv.resolution == 86400
        assert ohlcv.timestamp == datetime(2026, 1, 1, 0, 0, 0)
        assert ohlcv.open == EMoney(value=100 * 10**8, currency=Currency("BTC", 8))
        assert ohlcv.high == EMoney(value=120 * 10**8, currency=Currency("BTC", 8))
        assert ohlcv.low == EMoney(value=90 * 10**8, currency=Currency("BTC", 8))
        assert ohlcv.close == EMoney(value=110 * 10**8, currency=Currency("BTC", 8))
        assert ohlcv.volume == EMoney(value=1000 * 10**8, currency=Currency("BTC", 8))
        assert ohlcv.quote_volume == EMoney(value=2000 * 10**2, currency=Currency("USD", 2))

    def test_create_ohlcv_from_float(self):
        ohlcv = OHLCV.from_float(
            pair=Pair(base=Currency("BTC", 8), quote=Currency("USD", 2)),
            resolution=86400,
            timestamp=datetime(2026, 1, 1, 0, 0, 0),
            open=100.00,
            high=120.00,
            low=90.00,
            close=110.00,
            volume=1000.00,
            quote_volume=2000.00,
        )
        assert ohlcv.pair == Pair(base=Currency("BTC", 8), quote=Currency("USD", 2))
        assert ohlcv.resolution == 86400
        assert ohlcv.timestamp == datetime(2026, 1, 1, 0, 0, 0)
        assert ohlcv.open == EMoney(value=100 * 10**8, currency=Currency("BTC", 8))
        assert ohlcv.high == EMoney(value=120 * 10**8, currency=Currency("BTC", 8))
        assert ohlcv.low == EMoney(value=90 * 10**8, currency=Currency("BTC", 8))
        assert ohlcv.close == EMoney(value=110 * 10**8, currency=Currency("BTC", 8))
        assert ohlcv.volume == EMoney(value=1000 * 10**8, currency=Currency("BTC", 8))
        assert ohlcv.quote_volume == EMoney(value=2000 * 10**2, currency=Currency("USD", 2))

    @pytest.mark.parametrize(
        ("field_name", "value"),
        [
            ("pair", "not-a-pair"),
            ("resolution", "-1"),
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
