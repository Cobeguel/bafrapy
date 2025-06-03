from datetime import datetime

import pytest

import bafrapy.backtest.base as base
import bafrapy.backtest.dataset as dataset


class TestOrder:

    def test_create_market_order(self):
        order = base.MarketOrder(1, datetime(2024, 1, 1), base.Side.buy, 100)
        assert order.order_id == 1
        assert order.side == base.Side.buy
        assert order.create_time == datetime(2024, 1, 1)
        assert order.quantity == 100
        assert order.state == base.OrderState.pending

    def test_execute_market_order(self):
        order = base.MarketOrder(1, datetime(2024, 1, 1), base.Side.buy, 100)
        ohlcv = dataset.OHLCV(datetime(2024, 1, 2), 1, 1, 1, 1, 0)
        trade = order.execute(ohlcv).trade
        assert trade is not None
        assert order.state == base.OrderState.executed
        assert order.executed_time == datetime(2024, 1, 2)
        assert trade.order.order_id == 1

    def test_trade_market_order(self):
        order = base.MarketOrder(1, datetime(2024, 1, 1), base.Side.buy, 100)
        ohlcv = dataset.OHLCV(datetime(2024, 1, 2), 1, 1, 1, 1, 0)
        trade = order.execute(ohlcv).trade
        assert trade.order is order
        assert trade.quantity == 100
        assert trade.executed_price == 1
        assert trade.executed_time == datetime(2024, 1, 2)

    def test_create_limit_order(self):
        order = base.LimitOrder(1, datetime(2024, 1, 1), base.Side.buy, 100, 5)
        assert order.order_id == 1
        assert order.side == base.Side.buy
        assert order.create_time == datetime(2024, 1, 1)
        assert order.quantity == 100
        assert order.price == 5
        assert order.state == base.OrderState.pending

    def test_execute_buy_limit_order_under_price(self):
        order = base.LimitOrder(1, datetime(2024, 1, 1), base.Side.buy, 100, 5)
        ohlcv = dataset.OHLCV(datetime(2024, 1, 2), 1, 1, 1, 1, 0)
        result = order.execute(ohlcv)
        assert result is not None
        assert result.is_trade()
        trade = result.trade
        assert order.state == base.OrderState.executed
        assert order.executed_time == datetime(2024, 1, 2)
        assert trade.order.order_id == 1

    def test_not_execute_buy_limit_order_over_price(self):
        order = base.LimitOrder(1, datetime(2024, 1, 1), base.Side.buy, 100, 5)
        ohlcv = dataset.OHLCV(datetime(2024, 1, 10), 10, 10, 10, 10, 0)
        result = order.execute(ohlcv)
        assert result is None
        assert order.state == base.OrderState.pending