from decimal import Decimal

import pytest

from bafrapy.backtest.money import Currency, EMoney, Normalizer


class TestNormalizer:
    def test_normalize_positive_decimals(self):
        result = Normalizer.normalize_decimal(Decimal("1.00"), 2)
        assert result == 100

    def test_normaliza_zero_decimals(self):
        result = Normalizer.normalize_decimal(Decimal("1.00"), 0)
        assert result == 1

    def test_normalize_decimal_rounding_up(self):
        result = Normalizer.normalize_decimal(Decimal("1.006"), 2)
        assert result == 101

    def test_normalize_decimal_rounding_down(self):
        result = Normalizer.normalize_decimal(Decimal("1.004"), 2)
        assert result == 100

    def test_normalize_negative_decimals(self):
        with pytest.raises(ValueError):
            Normalizer.normalize_decimal(Decimal("1.00"), -1)

    def test_to_decimal(self):
        result = Normalizer.to_decimal(100, 2)
        assert result == Decimal("1.00")

    def test_normalize_float(self):
        result = Normalizer.normalize_float(1.00, 2)
        assert result == 100

    def test_normalize_float_non_float_value(self):
        with pytest.raises(TypeError):
            Normalizer.normalize_float(1, 2)

    def test_to_float(self):
        result = Normalizer.to_float(100, 2)
        assert result == 1.00


class TestEMoney:
    def test_create_zero_value(self):
        emoney = EMoney(value=0, currency=Currency("EUR"), decimals=2)
        assert emoney.value == 0
        assert emoney.currency == Currency("EUR")
        assert emoney.decimals == 2

    def test_create_zero_currency(self):
        emoney = EMoney.zero(currency=Currency("EUR"), decimals=2)
        assert emoney == EMoney(value=0, currency=Currency("EUR"), decimals=2)

    def test_create_from_decimal(self):
        emoney = EMoney.from_decimal(Decimal("1.23"), Currency("EUR"))
        assert emoney == EMoney(value=123, currency=Currency("EUR"), decimals=2)

    def test_create_from_float(self):
        emoney = EMoney.from_float(1.234, Currency("EUR"))
        assert emoney == EMoney(value=1234, currency=Currency("EUR"), decimals=3)

    def test_invalid_decimals(self):
        with pytest.raises(ValueError):
            EMoney(value=100, currency=Currency("EUR"), decimals=-1)

    def test_to_decimal(self):
        emoney = EMoney(value=100, currency=Currency("EUR"), decimals=2)
        assert emoney.to_decimal() == Decimal("1.00")

    def test_to_float(self):
        emoney = EMoney(value=100, currency=Currency("EUR"), decimals=2)
        assert emoney.to_float() == 1.00

    def test_sign_helpers(self):
        assert EMoney(value=-100, currency=Currency("EUR"), decimals=2).is_negative()
        assert EMoney(value=100, currency=Currency("EUR"), decimals=2).is_positive()
        assert EMoney(value=0, currency=Currency("EUR"), decimals=2).is_zero()

    def test_equality_aligns_decimals(self):
        m1 = EMoney(value=123, currency=Currency("BTC"), decimals=2)
        m2 = EMoney(value=1230, currency=Currency("BTC"), decimals=3)
        assert m1 == m2

    def test_comparisons_align_decimals(self):
        m1 = EMoney(value=123, currency=Currency("BTC"), decimals=2)
        m2 = EMoney(value=1234, currency=Currency("BTC"), decimals=3)
        assert m1 < m2
        assert m2 > m1
        assert m1 <= m2
        assert m2 >= m1

    def test_add_aligns_to_higher_decimals(self):
        m1 = EMoney(value=123, currency=Currency("BTC"), decimals=2)
        m2 = EMoney(value=1234, currency=Currency("BTC"), decimals=3)
        assert m1 + m2 == EMoney(value=2464, currency=Currency("BTC"), decimals=3)

    def test_sub_aligns_to_higher_decimals(self):
        m1 = EMoney(value=1234, currency=Currency("BTC"), decimals=3)
        m2 = EMoney(value=123, currency=Currency("BTC"), decimals=2)
        assert m1 - m2 == EMoney(value=4, currency=Currency("BTC"), decimals=3)

    def test_negative_preserves_decimals(self):
        money = EMoney(value=1234, currency=Currency("BTC"), decimals=3)
        assert -money == EMoney(value=-1234, currency=Currency("BTC"), decimals=3)

    @pytest.mark.parametrize("operation", ["eq", "lt", "le", "gt", "ge", "add", "sub"])
    def test_binary_operations_reject_different_symbols(self, operation):
        m1 = EMoney(value=100, currency=Currency("EUR"), decimals=2)
        m2 = EMoney(value=100, currency=Currency("USD"), decimals=2)

        with pytest.raises(TypeError):
            if operation == "eq":
                _ = m1 == m2
            elif operation == "lt":
                _ = m1 < m2
            elif operation == "le":
                _ = m1 <= m2
            elif operation == "gt":
                _ = m1 > m2
            elif operation == "ge":
                _ = m1 >= m2
            elif operation == "add":
                _ = m1 + m2
            else:
                _ = m1 - m2

    @pytest.mark.parametrize(
        ("multiplier", "expected_value"),
        [(2, 200), (2.0, 200), (Decimal("1.5"), 150)],
    )
    def test_mul_scalar_preserves_decimals(self, multiplier, expected_value):
        money = EMoney(value=100, currency=Currency("EUR"), decimals=2)
        assert money * multiplier == EMoney(value=expected_value, currency=Currency("EUR"), decimals=2)

    def test_rmul_scalar_preserves_decimals(self):
        money = EMoney(value=100, currency=Currency("EUR"), decimals=2)
        assert 2 * money == EMoney(value=200, currency=Currency("EUR"), decimals=2)

    @pytest.mark.parametrize(
        ("divisor", "expected_value"),
        [(2, 50), (2.0, 50), (Decimal("4"), 25)],
    )
    def test_div_scalar_preserves_decimals(self, divisor, expected_value):
        money = EMoney(value=100, currency=Currency("EUR"), decimals=2)
        assert money / divisor == EMoney(value=expected_value, currency=Currency("EUR"), decimals=2)

    def test_scalar_bool_is_rejected(self):
        money = EMoney(value=100, currency=Currency("EUR"), decimals=2)
        with pytest.raises(TypeError):
            _ = money * True

    def test_comparisons_with_int(self):
        money = EMoney(value=1234, currency=Currency("EUR"), decimals=2)
        assert money == 1234
        assert 1234 == money
        assert money > 34
        assert 34 < money
        assert money < 7216312
        assert 7216312 > money
        assert money >= 1234
        assert money <= 1234

    def test_int_comparison_ignores_decimals_metadata(self):
        money = EMoney(value=1234, currency=Currency("EUR"), decimals=2)
        assert money == 1234
        assert money != 12340

    @pytest.mark.parametrize("operation", ["eq", "lt", "le", "gt", "ge"])
    def test_int_comparison_rejects_bool(self, operation):
        money = EMoney(value=100, currency=Currency("EUR"), decimals=2)

        with pytest.raises(TypeError):
            if operation == "eq":
                _ = money == True
            elif operation == "lt":
                _ = money < True
            elif operation == "le":
                _ = money <= True
            elif operation == "gt":
                _ = money > True
            else:
                _ = money >= True
