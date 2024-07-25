import unittest

from datetime import datetime, timedelta

from loguru import logger

import bafrapy.backtest.base as base
import bafrapy.backtest.dataset as dataset

import pandas as pd

logger.remove()

class TestOrder(unittest.TestCase):

    def test_create_market_order(self):
        order = base.MarketOrder(1, datetime(2024, 1, 1), base.Side.buy, 100)
        self.assertEqual(order.order_id, 1)
        self.assertEqual(order.side, base.Side.buy)
        self.assertEqual(order.create_time, datetime(2024, 1, 1))
        self.assertEqual(order.quantity, 100)
        self.assertEqual(order.state, base.OrderState.pending)

    def test_execute_market_order(self):
        order = base.MarketOrder(1, datetime(2024, 1, 1), base.Side.buy, 100)
        ohlcv = dataset.OHLCV(datetime(2024, 1, 2), 1, 1, 1, 1, 0)
        trade = order.execute(ohlcv).trade
        self.assertIsNotNone(trade)
        self.assertEqual(order.state, base.OrderState.executed)
        self.assertEqual(order.executed_time, datetime(2024, 1, 2))
        self.assertEqual(trade.order.order_id, 1)

    def test_trade_market_order(self):
        order = base.MarketOrder(1, datetime(2024, 1, 1), base.Side.buy, 100)
        ohlcv = dataset.OHLCV(datetime(2024, 1, 2), 1, 1, 1, 1, 0)
        trade = order.execute(ohlcv).trade
        self.assertIs(trade.order, order)
        self.assertEqual(trade.quantity, 100)
        self.assertEqual(trade.executed_price, 1)
        self.assertEqual(trade.executed_time, datetime(2024, 1, 2))

    def test_create_limit_order(self):
        order = base.LimitOrder(1, datetime(2024, 1, 1), base.Side.buy, 100, 5)
        self.assertEqual(order.order_id, 1)
        self.assertEqual(order.side, base.Side.buy)
        self.assertEqual(order.create_time, datetime(2024, 1, 1))
        self.assertEqual(order.quantity, 100)
        self.assertEqual(order.price, 5)
        self.assertEqual(order.state, base.OrderState.pending)

    def test_execute_buy_limit_order_under_price(self):
        order = base.LimitOrder(1, datetime(2024, 1, 1), base.Side.buy, 100, 5)
        ohlcv = dataset.OHLCV(datetime(2024, 1, 2), 1, 1, 1, 1, 0)
        resultOrder = order.execute(ohlcv)
        self.assertIsNotNone(resultOrder)
        self.assertTrue(resultOrder.is_trade())
        trade = resultOrder.trade
        self.assertEqual(order.state, base.OrderState.executed)
        self.assertEqual(order.executed_time, datetime(2024, 1, 2))
        self.assertEqual(trade.order.order_id, 1)

    def test_not_execute_buy_limit_order_over_price(self):
        order = base.LimitOrder(1, datetime(2024, 1, 1), base.Side.buy, 100, 5)
        ohlcv = dataset.OHLCV(datetime(2024, 1, 10), 10, 10, 10, 10, 0)
        trade = order.execute(ohlcv)
        self.assertIsNone(trade)
        self.assertEqual(order.state, base.OrderState.pending)


class TestPosition(unittest.TestCase):

    def setUpPositionByMarketBuy(self) -> None:
        self.order = base.MarketOrder(1, datetime(2024, 1, 1), base.Side.buy, 100)
        ohlcv = dataset.OHLCV(datetime(2024, 1, 2), 5, 5, 5, 5, 0)
        self.trade = self.order.execute(ohlcv).trade
        
    def test_create_long_position(self):
        self.setUpPositionByMarketBuy()
        position = base.Position(1, self.trade)
        self.assertEqual(position.position_id, 1)
        self.assertEqual(position.state, base.PositionState.open)
        self.assertEqual(position.side, base.Side.buy)
        self.assertIn(self.trade, position.trades)
        self.assertEqual(len(position.orders), 0)
        self.assertEqual(position.quantity, 100)

    def test_close_long_position(self):
        self.setUpPositionByMarketBuy()
        position = base.Position(1, self.trade)
        close_order = base.MarketOrder(2, datetime(2024, 1, 3), base.Side.sell, 100)
        position.add_order(close_order)
        ohlcv = dataset.OHLCV(datetime(2024, 1, 3), 10, 10, 10, 10, 0)
        close_trade = close_order.execute(ohlcv).trade
        try:
            position.notify_trade(close_trade)
        except Exception as e:
            self.fail(f"Error: {e}")

        self.assertEqual(position.quantity, 0)
        self.assertEqual(position.state, base.PositionState.closed)
        self.assertEqual(position.orders[0].state, base.OrderState.executed)
        self.assertIn(close_trade, position.trades)


class TestVBroker(unittest.TestCase):

    def setUpFixedDataset(self, candles):
        candles = 5
        start_date = datetime(2024, 1, 1)
        dates = [start_date + timedelta(days=x) for x in range(candles)]
        data = {
            'Date': dates,
            'Open': [1]*candles,
            'High': [1]*candles,
            'Low': [1]*candles,
            'Close': [1]*candles,
            'Volume': [1000]*candles
        }
        df = pd.DataFrame(data)
        df['Date'] = pd.to_datetime(df['Date'])
        self.dataset = dataset.DataSetPandas(86400, data=df)
    
    def setUp(self) -> None:
        pass
    
    def test_basic_setup(self):
        self.setUpFixedDataset(5)
        config = base.VBrokerConfig(
            initial_money=1000,
            initial_quote=0,
            fee=0.01,
            data=self.dataset
        )
        vbroker = base.VBroker(config)
        self.assertEqual(vbroker.available_money, 1000)
        self.assertEqual(vbroker.available_quote, 0)
        self.assertEqual(vbroker.fee, 0.01)
        current_data = vbroker.current_data()
        self.assertIsNotNone(current_data)
        self.assertEqual(current_data.open, 1)
        self.assertEqual(current_data.high, 1)
        self.assertEqual(current_data.low, 1)
        self.assertEqual(current_data.close, 1)
        self.assertEqual(current_data.volume, 1000)

    def test_add_market_buy_order(self):
        self.setUpFixedDataset(5)
        config = base.VBrokerConfig(
            initial_money=1000,
            initial_quote=0,
            fee=0.01,
            data=self.dataset
        )
        vbroker = base.VBroker(config)
        vbroker._add_order(base.MarketOrder(0, vbroker.current_time, base.Side.buy, 100))
        self.assertEqual(len(vbroker.orders), 1)
        self.assertEqual(len(vbroker.created_orders), 1)
        order = vbroker.get_order(0)
        self.assertEqual(order.order_id, 0)
        self.assertEqual(order.side, base.Side.buy)
        self.assertEqual(order.create_time, vbroker.current_time)
        self.assertEqual(order.quantity, 100)
        self.assertEqual(order.state, base.OrderState.pending)

    def test_execute_market_buy_order(self):
        self.setUpFixedDataset(5)
        config = base.VBrokerConfig(
            initial_money=1000,
            initial_quote=0,
            fee=0.00,
            data=self.dataset
        )
        vbroker = base.VBroker(config)
        vbroker._add_order(base.MarketOrder(0, vbroker.current_time, base.Side.buy, 100))
        vbroker.next_data()
        self.assertEqual(len(vbroker.created_orders), 0)
        self.assertEqual(vbroker.available_money, 900)
        self.assertEqual(vbroker.available_quote, 100)

