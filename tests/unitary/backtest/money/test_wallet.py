import pytest

from bafrapy.backtest.money import Currency, EMoney, SpotWallet, Wallet


class TestWallet:
    def test_add_currency(self):
        wallet = Wallet()
        wallet.add_currency(Currency("BTC"))
        assert wallet.currencies == [Currency("BTC")]
        assert wallet.get_balance(Currency("BTC")) == EMoney(value=0, currency=Currency("BTC"), decimals=0)

    def test_add_balance(self):
        wallet = Wallet()
        wallet.add_balance(EMoney(value=100000000, currency=Currency("BTC"), decimals=8))
        assert wallet.get_balance(Currency("BTC")) == EMoney(value=100000000, currency=Currency("BTC"), decimals=8)

    def test_subtract_balance(self):
        wallet = Wallet()
        wallet.add_balance(EMoney(value=100000000, currency=Currency("BTC"), decimals=8))
        wallet.subtract_balance(EMoney(value=100000000, currency=Currency("BTC"), decimals=8))
        assert wallet.get_balance(Currency("BTC")) == EMoney(value=0, currency=Currency("BTC"), decimals=8)

    def test_add_balance_promotes_to_higher_decimals(self):
        wallet = Wallet()
        wallet.add_balance(EMoney(value=123, currency=Currency("BTC"), decimals=2))
        wallet.add_balance(EMoney(value=1234, currency=Currency("BTC"), decimals=3))
        assert wallet.get_balance(Currency("BTC")) == EMoney(value=2464, currency=Currency("BTC"), decimals=3)

    def test_subtract_balance_promotes_to_higher_decimals(self):
        wallet = Wallet()
        wallet.add_balance(EMoney(value=1234, currency=Currency("BTC"), decimals=3))
        wallet.subtract_balance(EMoney(value=123, currency=Currency("BTC"), decimals=2))
        assert wallet.get_balance(Currency("BTC")) == EMoney(value=4, currency=Currency("BTC"), decimals=3)

    def test_add_several_currencies(self):
        wallet = Wallet()
        wallet.add_currency(Currency("BTC"))
        wallet.add_currency(Currency("ETH"))
        assert wallet.currencies == [Currency("BTC"), Currency("ETH")]
        assert wallet.get_balance(Currency("BTC")) == EMoney(value=0, currency=Currency("BTC"), decimals=0)
        assert wallet.get_balance(Currency("ETH")) == EMoney(value=0, currency=Currency("ETH"), decimals=0)


class TestSpotWallet:
    def test_add_balance(self):
        wallet = SpotWallet()
        wallet.add_balance(EMoney(value=100000000, currency=Currency("BTC"), decimals=8))
        assert wallet.get_balance(Currency("BTC")) == EMoney(value=100000000, currency=Currency("BTC"), decimals=8)

    def test_add_balance_insufficient_balance(self):
        wallet = SpotWallet()
        wallet.add_balance(EMoney(value=100000000, currency=Currency("BTC"), decimals=8))
        with pytest.raises(ValueError):
            wallet.add_balance(EMoney(value=-200000000, currency=Currency("BTC"), decimals=8))

    def test_subtract_balance(self):
        wallet = SpotWallet()
        wallet.add_balance(EMoney(value=100000000, currency=Currency("BTC"), decimals=8))
        wallet.subtract_balance(EMoney(value=100000000, currency=Currency("BTC"), decimals=8))
        assert wallet.get_balance(Currency("BTC")) == EMoney(value=0, currency=Currency("BTC"), decimals=8)

    def test_subtract_balance_insufficient_balance_with_mixed_decimals(self):
        wallet = SpotWallet()
        wallet.add_balance(EMoney(value=123, currency=Currency("BTC"), decimals=2))
        with pytest.raises(ValueError):
            wallet.subtract_balance(EMoney(value=1234, currency=Currency("BTC"), decimals=3))
