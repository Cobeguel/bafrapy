import pytest

from bafrapy.backtest.money import Wallet, EMoney, Currency, SpotWallet


class TestWallet:
    def test_add_currency(self):
        wallet = Wallet()
        wallet.add_currency(Currency("BTC", 8))
        assert wallet.currencies == [Currency("BTC", 8)]

    def test_add_balance(self):
        wallet = Wallet()
        wallet.add_balance(EMoney(value=100000000, currency=Currency("BTC", 8)))
        assert wallet.get_balance(Currency("BTC", 8)) == EMoney(
            value=100000000, currency=Currency("BTC", 8)
        )

    def test_subtract_balance(self):
        wallet = Wallet()
        wallet.add_balance(EMoney(value=100000000, currency=Currency("BTC", 8)))
        wallet.subtract_balance(EMoney(value=100000000, currency=Currency("BTC", 8)))
        assert wallet.get_balance(Currency("BTC", 8)) == EMoney(
            value=0, currency=Currency("BTC", 8)
        )

    def test_add_several_currencies(self):
        wallet = Wallet()
        wallet.add_currency(Currency("BTC", 8))
        wallet.add_currency(Currency("ETH", 8))
        assert wallet.currencies == [Currency("BTC", 8), Currency("ETH", 8)]
        assert wallet.get_balance(Currency("BTC", 8)) == EMoney(
            value=0, currency=Currency("BTC", 8)
        )
        assert wallet.get_balance(Currency("ETH", 8)) == EMoney(
            value=0, currency=Currency("ETH", 8)
        )


class TestSpotWallet:
    def test_add_balance(self):
        wallet = SpotWallet()
        wallet.add_balance(EMoney(value=100000000, currency=Currency("BTC", 8)))
        assert wallet.get_balance(Currency("BTC", 8)) == EMoney(
            value=100000000, currency=Currency("BTC", 8)
        )

    def test_add_balance_insufficient_balance(self):
        wallet = SpotWallet()
        wallet.add_balance(EMoney(value=100000000, currency=Currency("BTC", 8)))
        with pytest.raises(ValueError):
            wallet.add_balance(EMoney(value=-200000000, currency=Currency("BTC", 8)))

    def test_subtract_balance(self):
        wallet = SpotWallet()
        wallet.add_balance(EMoney(value=100000000, currency=Currency("BTC", 8)))
        wallet.subtract_balance(EMoney(value=100000000, currency=Currency("BTC", 8)))
        assert wallet.get_balance(Currency("BTC", 8)) == EMoney(
            value=0, currency=Currency("BTC", 8)
        )

    def test_subtract_balance_insufficient_balance(self):
        wallet = SpotWallet()
        wallet.add_balance(EMoney(value=100000000, currency=Currency("BTC", 8)))
        with pytest.raises(ValueError):
            wallet.subtract_balance(
                EMoney(value=200000000, currency=Currency("BTC", 8))
            )
