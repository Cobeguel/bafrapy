import pytest

from bafrapy.backtest.money import Currency, EMoney, ERate


class TestERate:
    def test_convert_eur_jpy(self):
        eur = EMoney(value=300, currency=Currency("EUR"), decimals=2)
        jpy_rate = EMoney(value=180, currency=Currency("JPY"), decimals=0)
        jpy = ERate(Currency("EUR"), Currency("JPY")).convert(eur, jpy_rate)
        assert jpy == EMoney(value=540, currency=Currency("JPY"), decimals=0)

    def test_convert_btc_usd(self):
        btc = EMoney(value=200_000_000, currency=Currency("BTC"), decimals=8)  # 2 BTC
        usd_rate = EMoney(value=5_000_000, currency=Currency("USD"), decimals=2)  # 50_000 USD/BTC
        usd = ERate(Currency("BTC"), Currency("USD")).convert(btc, usd_rate)
        assert usd == EMoney(value=10_000_000, currency=Currency("USD"), decimals=2)  # 100_000 USD

    def test_convert_custom_target_decimals(self):
        eur = EMoney(value=300, currency=Currency("EUR"), decimals=2)
        jpy_rate = EMoney(value=180, currency=Currency("JPY"), decimals=0)
        jpy = ERate(Currency("EUR"), Currency("JPY")).convert(eur, jpy_rate, target_decimals=0)
        assert jpy == EMoney(value=540, currency=Currency("JPY"), decimals=0)

    def test_convert_rejects_wrong_amount_currency(self):
        usd = EMoney(value=100, currency=Currency("USD"), decimals=2)
        jpy_rate = EMoney(value=180, currency=Currency("JPY"), decimals=0)
        with pytest.raises(TypeError, match="Invalid amount currency"):
            ERate(Currency("EUR"), Currency("JPY")).convert(usd, jpy_rate)

    def test_convert_rejects_wrong_rate_currency(self):
        eur = EMoney(value=300, currency=Currency("EUR"), decimals=2)
        usd_rate = EMoney(value=100, currency=Currency("USD"), decimals=2)
        with pytest.raises(TypeError, match="Invalid rate currency"):
            ERate(Currency("EUR"), Currency("JPY")).convert(eur, usd_rate)

    def test_convert_rejects_non_emoney(self):
        eur = EMoney(value=300, currency=Currency("EUR"), decimals=2)
        jpy_rate = EMoney(value=180, currency=Currency("JPY"), decimals=0)
        rate = ERate(Currency("EUR"), Currency("JPY"))
        with pytest.raises(TypeError, match="Unsupported amount type"):
            rate.convert(300, jpy_rate)
        with pytest.raises(TypeError, match="Unsupported rate type"):
            rate.convert(eur, 180)
