from datetime import datetime, timedelta

import pandas as pd
import pytest

import bafrapy.backtest.base as base
import bafrapy.backtest.dataset as dataset


class TestVBroker:

    def setup_method(self):
        self.setUpFixedDataset(5)

    def setUpFixedDataset(self, candles):
        start_date = datetime(2024, 1, 1)
        dates = [start_date + timedelta(days=x) for x in range(candles)]
        df = pd.DataFrame({
            'Date': dates,
            'Open': [1] * candles,
            'High': [1] * candles,
            'Low': [1] * candles,
            'Close': [1] * candles,
            'Volume': [1000] * candles
        })
        df['Date'] = pd.to_datetime(df['Date'])
        self.dataset = dataset.DataSetPandas(86400, data=df)

    def test_basic_setup(self):
        config = base.VBrokerConfig(
            initial_money=1000,
            initial_quote=0,
            fee=0.01,
            data=self.dataset
        )
        vbroker = base.VBroker(config)
        assert vbroker.available_money == 1000
        assert vbroker.available_quote == 0
        assert vbroker.fee == 0.01
        current_data = vbroker.current_data()
        assert current_data.open == 1
        assert current_data.high == 1
        assert current_data.low == 1
        assert current_data.close == 1
        assert current_data.volume == 1000

    def test_add_market_buy_order(self):
        config = base.VBrokerConfig(
            initial_money=1000,
            initial_quote=0,
            fee=0.01,
            data=self.dataset
        )
        vbroker = base.VBroker(config)
        vbroker._add_order(base.MarketOrder(0, vbroker.current_time, base.Side.buy, 100))
        assert len(vbroker.orders) == 1
        assert len(vbroker.created_orders) == 1
        order = vbroker.get_order(0)
        assert order.order_id == 0
        assert order.side == base.Side.buy
        assert order.create_time == vbroker.current_time
        assert order.quantity == 100
        assert order.state == base.OrderState.pending

    def test_execute_market_buy_order(self):
        config = base.VBrokerConfig(
            initial_money=1000,
            initial_quote=0,
            fee=0.00,
            data=self.dataset
        )
        vbroker = base.VBroker(config)
        vbroker._add_order(base.MarketOrder(0, vbroker.current_time, base.Side.buy, 100))
        vbroker.next_data()
        assert len(vbroker.created_orders) == 0
        assert vbroker.available_money == 900
        assert vbroker.available_quote == 100
