import pytest

from bafrapy.backtest.money import EMoney, Currency, Normalizer
from decimal import Decimal


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

    def test_normalize_float_rounding_up(self):
        result = Normalizer.normalize_float(1.006, 2)
        assert result == 101

    def test_normalize_float_rounding_down(self):
        result = Normalizer.normalize_float(1.004, 2)
        assert result == 100

    def test_normalize_float_negative_decimals(self):
        with pytest.raises(ValueError):
            Normalizer.normalize_float(1.00, -1)

    def test_normalize_float_non_float_value(self):
        with pytest.raises(TypeError):
            Normalizer.normalize_float(1, 2)

    def test_normalize_float_non_int_value(self):
        with pytest.raises(TypeError):
            Normalizer.normalize_float(Decimal("1.00"), 2)

    def test_to_float(self):
        result = Normalizer.to_float(100, 2)
        assert result == 1.00


class TestEMoney:
    def test_create_zero_value(self):
        emoney = EMoney(value=0, currency=Currency("EUR", 2))
        assert emoney.value == 0
        assert emoney.currency == Currency("EUR", 2)

    def test_create_zero_currency(self):
        emoney = EMoney.zero(currency=Currency("EUR", 2))
        assert emoney.value == 0
        assert emoney.currency == Currency("EUR", 2)

    def test_create_from_decimal(self):
        amount = Decimal("1.00")
        currency = Currency("EUR", 2)
        emoney = EMoney.from_decimal(amount, currency)
        assert emoney.value == 100
        assert emoney.currency == currency

    def test_create_from_float(self):
        amount = 1.00
        currency = Currency("EUR", 2)
        emoney = EMoney.from_float(amount, currency)
        assert emoney.value == 100
        assert emoney.currency == currency

    def test_to_decimal(self):
        emoney = EMoney(value=100, currency=Currency("EUR", 2))
        assert emoney.to_decimal() == Decimal("1.00")

    def test_to_float(self):
        emoney = EMoney(value=100, currency=Currency("EUR", 2))
        assert emoney.to_float() == 1.00

    def test_is_negative_with_negative_value(self):
        emoney = EMoney(value=-100, currency=Currency("EUR", 2))
        assert emoney.is_negative() is True

    def test_is_negative_with_positive_value(self):
        emoney = EMoney(value=100, currency=Currency("EUR", 2))
        assert emoney.is_negative() is False

    def test_is_positive(self):
        emoney = EMoney(value=100, currency=Currency("EUR", 2))
        assert emoney.is_positive() is True

    def test_is_positive_with_negative_value(self):
        emoney = EMoney(value=-100, currency=Currency("EUR", 2))
        assert emoney.is_positive() is False

    def test_is_zero(self):
        emoney = EMoney(value=0, currency=Currency("EUR", 2))
        assert emoney.is_zero() is True

    def test_is_zero_with_non_zero_value(self):
        emoney1 = EMoney(value=100, currency=Currency("EUR", 2))
        emoney2 = EMoney(value=-100, currency=Currency("EUR", 2))
        assert emoney1.is_zero() is False and emoney2.is_zero() is False

    def test_equality_are_equal(self):
        m1 = EMoney(value=100, currency=Currency("EUR", 2))
        m2 = EMoney(value=100, currency=Currency("EUR", 2))
        assert m1 == m2

    def test_equality_are_not_equal_different_value(self):
        m1 = EMoney(value=100, currency=Currency("EUR", 2))
        m2 = EMoney(value=200, currency=Currency("EUR", 2))
        assert m1 != m2

    def test_equality_are_not_equal_different_currency(self):
        m1 = EMoney(value=100, currency=Currency("EUR", 2))
        m2 = EMoney(value=100, currency=Currency("USD", 2))
        with pytest.raises(TypeError):
            _ = m1 != m2

    def test_less_than(self):
        m1 = EMoney(value=100, currency=Currency("EUR", 2))
        m2 = EMoney(value=200, currency=Currency("EUR", 2))
        assert m1 < m2

    def test_less_than_with_greater_value(self):
        m1 = EMoney(value=200, currency=Currency("EUR", 2))
        m2 = EMoney(value=100, currency=Currency("EUR", 2))
        assert not (m1 < m2)

    def test_less_than_different_currency(self):
        m1 = EMoney(value=100, currency=Currency("EUR", 2))
        m2 = EMoney(value=200, currency=Currency("USD", 2))
        with pytest.raises(TypeError):
            m1 < m2

    def test_less_than_or_equal(self):
        m1 = EMoney(value=100, currency=Currency("EUR", 2))
        m2 = EMoney(value=200, currency=Currency("EUR", 2))
        assert m1 <= m2

    def test_less_than_or_equal_with_greater_value(self):
        m1 = EMoney(value=200, currency=Currency("EUR", 2))
        m2 = EMoney(value=100, currency=Currency("EUR", 2))
        assert not (m1 <= m2)

    def test_less_than_or_equal_different_currency(self):
        m1 = EMoney(value=100, currency=Currency("EUR", 2))
        m2 = EMoney(value=200, currency=Currency("USD", 2))
        with pytest.raises(TypeError):
            m1 <= m2

    def test_greater_than(self):
        m1 = EMoney(value=200, currency=Currency("EUR", 2))
        m2 = EMoney(value=100, currency=Currency("EUR", 2))
        assert m1 > m2

    def test_greater_than_with_lesser_value(self):
        m1 = EMoney(value=100, currency=Currency("EUR", 2))
        m2 = EMoney(value=200, currency=Currency("EUR", 2))
        assert not (m1 > m2)

    def test_greater_than_different_currency(self):
        m1 = EMoney(value=200, currency=Currency("EUR", 2))
        m2 = EMoney(value=100, currency=Currency("USD", 2))
        with pytest.raises(TypeError):
            m1 > m2

    def test_greater_than_or_equal(self):
        m1 = EMoney(value=200, currency=Currency("EUR", 2))
        m2 = EMoney(value=100, currency=Currency("EUR", 2))
        assert m1 >= m2

    def test_greater_than_or_equal_with_lesser_value(self):
        m1 = EMoney(value=100, currency=Currency("EUR", 2))
        m2 = EMoney(value=200, currency=Currency("EUR", 2))
        assert not (m1 >= m2)

    def test_greater_than_or_equal_different_currency(self):
        m1 = EMoney(value=200, currency=Currency("EUR", 2))
        m2 = EMoney(value=100, currency=Currency("USD", 2))
        with pytest.raises(TypeError):
            m1 >= m2

    def test_add(self):
        m1 = EMoney(value=100, currency=Currency("EUR", 2))
        m2 = EMoney(value=200, currency=Currency("EUR", 2))
        m3 = m1 + m2
        assert m3.value == 300
        assert m3.currency == Currency("EUR", 2)

    def test_add_different_currency(self):
        m1 = EMoney(value=100, currency=Currency("EUR", 2))
        m2 = EMoney(value=200, currency=Currency("USD", 2))
        with pytest.raises(TypeError):
            m1 + m2

    def test_negative_add_positive_result(self):
        m1 = EMoney(value=200, currency=Currency("EUR", 2))
        m2 = EMoney(value=-100, currency=Currency("EUR", 2))
        m3 = m1 + m2
        assert m3.value == 100
        assert m3.currency == Currency("EUR", 2)

    def test_negative_add_negative_result(self):
        m1 = EMoney(value=100, currency=Currency("EUR", 2))
        m2 = EMoney(value=-200, currency=Currency("EUR", 2))
        m3 = m1 + m2
        assert m3.value == -100
        assert m3.currency == Currency("EUR", 2)

    def test_sub_positive_result(self):
        m1 = EMoney(value=100, currency=Currency("EUR", 2))
        m2 = EMoney(value=200, currency=Currency("EUR", 2))
        m3 = m1 - m2
        assert m3.value == -100

    def test_sub_negative_result(self):
        m1 = EMoney(value=100, currency=Currency("EUR", 2))
        m2 = EMoney(value=200, currency=Currency("EUR", 2))
        m3 = m1 - m2
        assert m3.value == -100

    def test_sub_positive_result_with_negative_value(self):
        m1 = EMoney(value=-100, currency=Currency("EUR", 2))
        m2 = EMoney(value=200, currency=Currency("EUR", 2))
        m3 = m1 - m2
        assert m3.value == -300
        assert m3.currency == Currency("EUR", 2)

    def test_sub_negative_result_with_positive_value(self):
        m1 = EMoney(value=100, currency=Currency("EUR", 2))
        m2 = EMoney(value=-200, currency=Currency("EUR", 2))
        m3 = m1 - m2
        assert m3.value == 300
        assert m3.currency == Currency("EUR", 2)

    def test_sub_different_currency(self):
        m1 = EMoney(value=100, currency=Currency("EUR", 2))
        m2 = EMoney(value=200, currency=Currency("USD", 2))
        with pytest.raises(TypeError):
            _ = m1 - m2

    def test_mul_escalar_int(self):
        m1 = EMoney(value=100, currency=Currency("EUR", 2))
        m2 = 2
        m3 = m1 * m2
        assert m3.value == 200
        assert m3.currency == Currency("EUR", 2)

    def test_mul_escalar_float(self):
        m1 = EMoney(value=100, currency=Currency("EUR", 2))
        m2 = 2.0
        m3 = m1 * m2
        assert m3.value == 200
        assert m3.currency == Currency("EUR", 2)

    def test_mul_escalar_decimal(self):
        m1 = EMoney(value=100, currency=Currency("EUR", 2))
        m2 = Decimal("2.0")
        m3 = m1 * m2
        assert m3.value == 200
        assert m3.currency == Currency("EUR", 2)

    def test_mul_escalar_bool(self):
        m1 = EMoney(value=100, currency=Currency("EUR", 2))
        m2 = True
        with pytest.raises(TypeError):
            _ = m1 * m2

    def test_mul_money_unsupported_type(self):
        m1 = EMoney(value=100, currency=Currency("EUR", 2))
        m2 = EMoney(value=200, currency=Currency("USD", 2))
        with pytest.raises(TypeError):
            _ = m1 * m2
