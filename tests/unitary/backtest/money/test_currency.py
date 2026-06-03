from bafrapy.backtest.money import Currency


class TestCurrency:
    def test_symbol(self):
        c = Currency("BTC")
        assert c.symbol == "BTC"

    def test_equality_same_symbol(self):
        assert Currency("BTC") == Currency("BTC")

    def test_inequality_different_symbol(self):
        assert Currency("BTC") != Currency("ETH")
