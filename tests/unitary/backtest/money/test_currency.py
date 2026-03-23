from bafrapy.backtest.money import Currency


class TestCurrency:
    def test_symbol_and_decimals(self):
        c = Currency("BTC", 8)
        assert c.symbol == "BTC"
        assert c.decimals == 8

    def test_equality_same_symbol_and_decimals(self):
        assert Currency("BTC", 8) == Currency("BTC", 8)

    def test_inequality_different_symbol(self):
        assert Currency("BTC", 8) != Currency("ETH", 8)

    def test_inequality_different_decimals(self):
        assert Currency("BTC", 8) != Currency("BTC", 18)
