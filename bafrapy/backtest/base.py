from abc import ABC, ABCMeta, abstractmethod
from collections import OrderedDict
from dataclasses import InitVar, dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import ClassVar, Dict, List, OrderedDict

import numpy as np
import pandas as pd

from bafrapy.logger import LogField, LoguruLogger as log

from bafrapy.backtest.dataset import OHLCV, DataSet
from bafrapy.backtest.exceptions import *


class Side(Enum):
    """
    Enum to represent the side of an order.
    """
    buy = 1    #: Represents a long order.
    sell = 2   #: Represents a short order.

class Pair(Enum):
    """
    Enum to represent the pair of a trade of the form A/B.
    """
    base = 1    #: Represent the base instrument as A in the pair A/B.
    quote = 2   #: Represent the quote instrument as B in the pair A/B.
    

class OrderType(Enum):
    """
    Enum to represent the type of an order.
    """
    market = 1      #: Represents a market order.
    limit = 2       #: Represents a limit order. 
    stop_limit = 3  #: Represents a stop limit order.


class OrderState(Enum):
    """
    Enum to represent the state of an order.
    """
    created = 0                       #: Represents an order that has been created but not yet checked.
    pending = 1                       #: Represents an open order.
    rejected = 2                      #: A created order that cannot be processsed. 
    # pre_processed = 2               #: Represents an order that has been pre_processed. It may be canceled.
    pre_executed = 4                #: Represents a processed but not totally validated order. It may be canceled.
    executed = 4                      #: Represents a processed an valid order.
    canceled = 5                      #: Represents a canceled order.
    partially_executed = 6            #: Represents a partially completed composite order.


@dataclass
class Order(ABC):
    """
    Class to represent an order in the trading system.
    """

    #: Id of the order
    order_id: int

    #: Creation time in backtest context. It refers to the time within the related candle.
    create_time: datetime

    #: Time when the order was executed. Usually is related to an open candle datetime.
    executed_time: datetime = field(default=None, init=False)

    #: Time when the order was canceled.
    cancel_time: datetime = field(default=None, init=False)

    #: State of the order.
    state: OrderState = field(default=OrderState.pending, init=False)

    def is_open(self):
        """
        Indicates whether the order is currently open.
        """
        return self.state == OrderState.pending

    def is_executed(self):
        """
        Indicates whether the order is currently executed.
        """
        return self.state == OrderState.executed and self.executed_time is not None

    def is_canceled(self):
        """
        Indicates whether the order is currently canceled.
        """
        return self.state == OrderState.canceled
    

    def cancel(self, time: datetime) -> bool:
        """
        Cancel an order.

        Args:
            time (datetime): Time when the order is canceled.

        Returns:
            bool: True if the order was canceled, False otherwise.
        """
        if self.state == OrderState.pending or self.state == OrderState.created:
            self.state = OrderState.canceled
            self.cancel_time = time
            return True
        return False

    def execute(self, ohlcv: OHLCV, **kwargs) -> 'ResultOrder':
        """
        Execute an order. The order must be in an open state to be executed.
        The way the order is executed depends on the type of the order.

        Args:
            ohlcv (OHLCV): Current candle.

        Returns:
            ResultOrder
        """
        if self.state is not OrderState.pending:
            raise ValueError("an order must be open to be executed")
         
        result = self.process(ohlcv, **kwargs)
        if result is None:
            return None

        # The order is simple and was executed
        if result.is_trade() and result.trade is not None:
            if self.state is not OrderState.executed or self.state != OrderState.pre_executed:
                raise InvalidStateExecutedSimpleOrder(self.order_id)

        else: 
            self.executed_time = ohlcv.timestamp
            self.state = OrderState.partially_executed

        self.executed_time = ohlcv.timestamp
        return result
    
    def validate(self) -> None:
        self.state = OrderState.executed

    def reject(self) -> None:
        self.state = OrderState.rejected

    @abstractmethod
    def process(self, ohlcv: OHLCV, **kwargs) -> 'ResultOrder':
        """
        Process the order. This method must be implemented by the subclasses.

        Args:
            ohlcv (OHLCV): Current ohlcv of the instrument.

        Returns:
            ResultOrder: Result of the order. If cannot be executed, return None.
        """
        pass


@dataclass 
class SimpleOrder(Order):
    #: Side of the order
    side: Side

    #: Amount of units. The meaning of the quantity depends on the order type.
    quantity: Decimal

    @abstractmethod 
    def required_money(self, current_ohlcv: OHLCV) -> Decimal:
        """
        Calculate the required money to execute the order.
        """
        pass


@dataclass
class CompositeOrder(Order):
    #: List of children orders.
    children_orders: List[Order] = field(default_factory=list, init=False)

    def cancel_order(self):
        for order in self.children_orders:
            if order.state == OrderState.pending:
                order.cancel()


class OrderExecutionCriteria(Enum):
    all_candle = 1
    on_open = 2
    on_current_close = 3


@dataclass
class MarketOrder(SimpleOrder):
    """
    Class to represent a market order.
    """
    #: Execution criteria
    criteria: ClassVar[OrderExecutionCriteria] = OrderExecutionCriteria.on_open

    def __post_init__(self):
        if self.quantity <= 0:    
            raise ValueError("ammount to buy/sell must be greater than 0")
        
        log().debug(f"market order {self.order_id} created: {self.create_time}")
        
    def process(self, ohlcv: OHLCV, **kwargs) -> 'ResultOrder':
        """
        Implement the process method for a market order. A market order is executed at the current price.
        For more information about the method, see the Order class.
        """
        self.state = OrderState.pre_executed
        self.executed_time = ohlcv.timestamp
        executed_price = ohlcv.open if self.__class__.criteria == OrderExecutionCriteria.on_open else ohlcv.close
        return ResultOrder(trade=Trade(self, self.quantity, executed_price, ohlcv.timestamp))
    
    def required_money(self, current_ohlcv: OHLCV) -> Decimal:
        return current_ohlcv.close * self.quantity


@dataclass
class MarketOrderQuote(SimpleOrder):

    def __post_init__(self):
        if self.quantity <= 0:
            raise ValueError("order cannot be set with negative currency")
        
        log().debug(f"market order {self.order_id} created: {self.create_time}")
        
    def process(self, ohlcv: OHLCV, **kwargs) -> 'ResultOrder':
        self.state = OrderState.pre_executed
        self.executed_time = ohlcv.timestamp
        return ResultOrder(trade=Trade(self, self.quantity/ohlcv.close, ohlcv.close, ohlcv.timestamp))

@dataclass
class LimitOrder(SimpleOrder):
    """
    Class to represent a limit order.
    """

    #: Quantity of units.
    quantity: Decimal

    #: Price required to execute the order.
    price: Decimal = field(default=Decimal(0)) # 0 means market order
    
    # position_target: int = field(default=0) # if apply to a current opened position
    take_profit: Decimal = Decimal(0)
    stop_loss: Decimal = Decimal(0)

    def __post_init__(self):
       if self.price < 0:
            raise ValueError("price cannot be negative")
       
       log().debug(f"limit order {self.order_id} created: {self.create_time}")


    @property
    def required_money(self) -> Decimal:
        """
        Calculate the required money to execute the order.
        """
        return self.quantity * self.price

    def process(self, ohlcv: OHLCV, **kwargs) -> 'ResultOrder':
        """
        Implement the process method for a limit order. A limit order is executed when the current candle
        reaches the price of the order.
        """

        if self.side == Side.buy:
            if ohlcv.low > self.price:
                return None
        else:
            if ohlcv.high < self.price:
                return None

        self.state = OrderState.executed
        return ResultOrder(trade=Trade(self, self.quantity, self.price, ohlcv.timestamp))
        

@dataclass
class Trade:
    """
    Class to represent a trade in the trading system.
    """
    order: SimpleOrder
    quantity: Decimal
    executed_price: Decimal
    executed_time: datetime

    def __post_init__(self):
        if self.order.state != OrderState.executed or self.order.state != OrderState.pre_executed:
            raise ValueError("order must be executed")
            
        log().debug(f"Trade created: {self.executed_time}")

    @property        
    def side(self) -> Side:
        """
        Get the side of the trade based on the side of the order.
        """
        return self.order.side
    
    def money_traded(self) -> Decimal:
        """
        Calculate the required money to execute the order.
        """
        return self.quantity * self.executed_price


@dataclass
class ResultOrder:
    """
    Class to represent the result of an order. The result may contain a trade or another order but not both.
    """
    order: Order = field(default=None)
    trade: Trade = field(default=None)

    def __post_init__(self):
        if self.order is not None and self.trade is not None:
            raise ValueError("result must contain only an order or a trade")

    def is_trade(self) -> bool:
        """
        Indicates whether the result contains a trade.
        """
        return self.trade is not None
    
    def is_order(self) -> bool:
        """
        Indicates whether the result contains an order.
        """
        return self.order is not None


class PositionState(Enum):
    """
    Enum to represent the state of a position.
    """
    open = 1
    closed = 2


@dataclass
class Position:
    """
    Class to represent a position in the trading system.
    """
    #: Id of the position
    position_id: int

    #: To create a position a trade must be passed.
    initial_trade: InitVar[Trade]

    #: Amount of units. If buy, quote, if sell, base.
    quantity: Decimal = field(default=Decimal(0), init=False)

    #: Amount of units reserved for orders. Usually is related to the contrapart side of the position.
    reserved_quantity: Decimal = field(default=Decimal(0), init=False)

    #: State of the position
    state: PositionState = field(default=PositionState.open, init=False)

    #: Side of the position
    side: Side = field(default=None, init=False)

    #: List of pending orders related to the position.
    orders: List[Order] = field(default_factory=list, init=False)

    #: List of trades related to executed order of the position.
    trades: List[Trade] = field(default_factory=list, init=False)

    def __post_init__(self, init_trade: Trade):
        """
        Post-initialization to set up the position based on the initial trade.

        Args:
            initial_trade (Trade): The initial trade used to create the position.
        """
        if init_trade is None:
            raise ValueError("initial_order is required")
        
        self.trades.append(init_trade)
        self.quantity += init_trade.quantity
        order = init_trade.order # type: SimpleOrder
        self.side = order.side
        log().debug(f"Position created with trade made by order {init_trade.order.order_id} at {init_trade.executed_time}")

    def is_closed(self) -> bool:
        """
        Indicates whether the position is currently open.

        Returns:
            bool: True if the position is open, False otherwise.
        """
        return self.state == PositionState.closed

    def _is_side_reverse(self, side: Side) -> bool:
        """
        Indicates whether the side is reversed from the position side.

        Args:
            side (Side): Side to check.

        Returns:
            bool: True if the side is reversed, False otherwise.
        """
        return self.side != side 
    
    def _check_close_position(self) -> bool:
        """
        Check if the position must be closed and close it if necessary.
        """
        if self.quantity == 0 and len(self.pending_orders()) == 0 and self.state != PositionState.closed:
            self.state = PositionState.closed
            return True
        return False
    
    def add_order(self, order: Order):
        """
        Add a pending order to the position. The order must be open.
        """
        if order.state != OrderState.pending:
            raise ValueError("order must be open")        
        if order.order_id in [id for id in self.orders]:
            raise ValueError(f"order with {id} already exists in position {self.position_id}")
        self.orders.append(order)
        if self._is_side_reverse(order.side):
            self.reserved_quantity += order.quantity

    def notify_trade(self, trade: Trade):
        if trade.order not in self.orders:
            raise ValueError("trade not related to position")
        self.trades.append(trade)
        if self._is_side_reverse(trade.side):
            self.reserved_quantity -= trade.quantity
            self.quantity -= trade.quantity
        else:
            self.quantity += trade.quantity
        
        self._check_close_position()

    def pending_orders(self) -> List[Order]:
        """
        Get the pending orders related to the position.

        Returns:
            List[Order]: List of pending orders.
        """
        return [order for order in self.orders if order.state == OrderState.pending]
    
    def active_orders(self) -> List[Order]:
        """
        Get the active orders related to the position.

        Returns:
            List[Order]: List of active orders.
        """
        return [order for order in self.orders if order.state == OrderState.pending]

    def get_trades(self) -> List[Trade]:
        """
        Get the trades related to the position.

        Returns:
            List[Trade]: List of trades.
        """
        return self.trades

    def get_average_price(self) -> Decimal:
        """
        Get the average price of the position.

        Returns:
            Decimal: Average price of the position.
        """
        self.get_trades()
        if len(self.get_trades()) == 0:
            raise ValueError("no trades")

        return sum([trade.executed_price for trade in self.get_trades()]) / len(self.get_trades())


@dataclass
class VBrokerConfig:
    initial_money: Decimal = field(default=Decimal(0))
    initial_quote: Decimal = field(default=Decimal(0))
    fee: Decimal = field(default=Decimal(0))
    data: DataSet = field(default=None)


@dataclass
class VBroker:
    """
    Class to represent a broker in the trading system.
    """
    config: InitVar[VBrokerConfig]

    #: Money available in the broker.
    available_money: Decimal = field(default=Decimal(0), init=False)

    #: Reserved money in the broker. Usually used in buy orders.
    reserved_money: Decimal = field(default=Decimal(0), init=False)

    #: Quote available in the broker.
    available_quote: Decimal = field(default=Decimal(0), init=False)

    #: Reserved quote in the broker. Usually used in sell orders.
    reserved_quote: Decimal = field(default=Decimal(0), init=False)

    #: Fee to be applied to the broker.
    fee: Decimal = field(default=Decimal(0), init=False)

    #: List of all the orders in the broker.
    orders : List[Order] = field(default_factory=list, init=False)

    #: List of all new children orders in the broker.
    new_children_orders: List[Order] = field(default_factory=list, init=False)

    #: List of all created but unchecked orders in the broker.
    created_orders: OrderedDict[int, Order] = field(default_factory=OrderedDict, init=False)

    #: List of all rejected orders in the broker.
    rejected_orders: OrderedDict[int, Order] = field(default_factory=OrderedDict, init=False)

    #: List of all the pending orders in the broker. Is a subset of the orders.
    pending_orders: OrderedDict[int, Order] = field(default_factory=OrderedDict, init=False)

    #: List of all cancelled orders in the broker.
    canceled_orders: OrderedDict[int, Order] = field(default_factory=OrderedDict, init=False)
    
    #: List of all the closed orders in the broker.
    executed_orders: Dict[int, Order] = field(default_factory=dict, init=False)

    #: List of all trades as result of executed orders.
    trades: List[Trade] = field(default_factory=list, init=False)

    #: Current active position
    open_position: Position = field(default=None, init=False)

    #: List of all the closed positions in the broker.
    closed_positions: List[Position] = field(default_factory=list, init=False)
    
    #: Historical dataset used to backtest.
    _data: DataSet = field(default=None, init=False)

    #: Current data in the broker.
    _current_data: OHLCV = field(default=None, init=False)

    #: Last order id used to create a new order.
    _next_order_id: int = field(default=0, init=False)

    #: Last trade id used to create a new trade.
    _next_trade_id: int = field(default=0, init=False)

    #: Last position id used to create a new position.
    _next_position_id: int = field(default=0, init=False)

    last_exceptions: List[Exception] = field(default_factory=list, init=False)

    #: Last ohlcv
    _last_ohlcv: OHLCV = field(default=None, init=False)
    
    def __post_init__(self, config: VBrokerConfig):
        self._data = config.data
        self.available_money = config.initial_money
        self.available_quote = config.initial_quote
        self.fee = config.fee
        self._next_data()

    @property
    def current_time(self) -> datetime:
        """
        Get the current time in the broker.
        """
        return self._current_data.timestamp
    
    @property
    def total_money(self) -> Decimal:
        """
        Get total money in the broker (available + reserved).
        """
        return self.available_money + self.reserved_money
    
    # @property
    # def reserved_money(self) -> Decimal:
    #     """
    #     Get the reserved money in the broker.
    #     """
    #     pass
    
    @property
    def total_quote(self) -> Decimal:
        """
        Get total quote in the broker (available + reserved).
        """
        return self.available_quote + self.reserved_quote
        
    def _next_data(self) -> OHLCV:
        """
        Get the next data from the dataset.
        """
        if self._data is None:
            raise ValueError("data is not set")
        self._last_ohlcv = self._current_data
        self._current_data = self._data.next_data()

    def set_commision(self, commission: Decimal):
        """
        Set the commission to be applied to the broker.
        """
        if commission < 0:
            raise ValueError("broker commissions cannot be negative")
        
    def set_dataset(self, data: DataSet):
        """
        Set the dataset to be used by the broker.
        """
        self._data = data
        self._next_data()

    def next_data(self) -> OHLCV:
        """
        Get the next data from the dataset. This method must be used in the backtest loop.
        """

        self.last_exceptions.clear()
        self._next_data()
        self.pending_orders.update(self.created_orders)
        self.created_orders.clear()
        self._process_orders(self.pending_orders)
        return self._current_data
    
    def current_data(self) -> OHLCV:
        """
        Get the current data in the broker.
        """
        return self._current_data
    
    def get_order(self, id: int) -> Order:
        """
        Get an order by id.
        """
        try: 
            return self.orders[id]
        except IndexError:
            return None
    
    def add_money(self, money: float):
        """
        Add money to the broker.
        """
        if money < 0:
            raise ValueError("cannot add negative money")
        self.available_money += Decimal(money)

    def extract_money(self, money: float):
        """
        Simulate the extraction of money from the broker. 
        """
        if money < 0:
            raise ValueError("cannot extract negative money")
        # TODO: improve available money check
        if self.available_money < Decimal(money) or self.available_money == 0:
            raise ValueError("broker has not enough money")
        self.available_money -= Decimal(money)

    # Study if process market orders inmediately with a config parameter on_current_data_trade
    def _add_order(self, order: Order):
        """
        Add an open order to the broker. All orders are added by only checking the state.
        If the order cannot be executed (ej not enough money), this conflict is resolved
        within the process orders method.
        
        Args:
            order (Order): Order to be added.
        """
        if order.order_id in self.pending_orders:
            raise OrderAlreadyExists(order.order_id)
        self.orders.append(order)
        # self.pending_orders[order.order_id]=order
        self.created_orders[order.order_id] = order

    def cancel_order(self, order_id: str) -> bool:
        if order_id not in self.pending_orders:
            return False

    def create_order(self,
                    side: Side,
                    type: OrderType,
                    quantity: Decimal,
                    price: Decimal = 0,
                    take_profit: Decimal = 0,
                    stop_loss: Decimal = 0) -> OrderState:
        self._next_order_id += 1
        return self._add_order(Order(self._next_order_id, side, type, quantity, price, take_profit, stop_loss))

    def _process_order(self, order: Order, price: Decimal, time: datetime):
        """
        Process an specific order.
        """
        trade = order.execute(self._current_data.close, self._current_data.timestamp)
        if self.open_position is not None:
            self.open_position.add_order(trade)
        else:
            self.open_position = Position(trade)
        self.executed_orders[order.order_id] = self.pending_orders.pop(order.order_id)
        self.trades.append(trade)

    
    def _process_orders(self, pending_orders: OrderedDict[int, Order]):
        """
        Process all pending orders. This method is called within next_data method.
        Tries to execute all pending orders. As a pending order could generate a new order, 
        it is processed recursively but only with children orders in every new iteration.

        There are several issues to consider:
        - There are orders unchecked, so they must be canceled.
        - The reserved money for market orders is unknown.
        """

        log().debug(f"number of orders to process: {len(pending_orders)}")
        processed_orders = []
        new_orders = [] # Orders as result from composite orders
        for order_id in self.pending_orders.keys():
            order = self.pending_orders[order_id]
            log().debug(f"order to process: {type(order)} - {order.order_id}")
            if self.pending_orders[order_id].state != OrderState.pending:
                raise NewOrderNotOpen(order.order_id)

            result = order.process(self._current_data)
            if result is None:
                continue

            if result.is_trade(): # That means the order is simple
                if self.open_position is not None:
                    self.open_position.notify_trade(result.trade)  
                # If there is no open position, create a new one
                else:
                    self.open_position = Position(self._next_position_id, result.trade)
                    self._next_position_id += 1       
                # In both cases we have a position with a trade notified
                  
                self.trades.append(result.trade)

                # Adjust money and quote according to the order side
                order = result.trade.order # type: SimpleOrder
                if order.side == Side.buy:
                    # If the order is a market order, the reserved money is not known
                    if isinstance(order, MarketOrder):
                        if self.available_money < result.trade.money_traded():
                            raise NotEnoughMoneyToExecuteMarketOrder(order.order_id)
                        self.available_money -= result.trade.money_traded()
                    else:
                        self.reserved_money -= result.trade.money_traded()
                    self.available_quote += result.trade.quantity
                        
                else: # Side.sell
                    if isinstance(order, MarketOrder):
                        if self.available_quote < result.trade.quantity:
                            order.state = OrderState.rejected
                            raise NotEnoughQuoteToExecuteMarketOrder(order.order_id)
                        self.available_quote -= result.trade.quantity
                    else:
                        self.reserved_quote -= result.trade.quantity
                    self.available_money += result.trade.money_traded()
                processed_orders.append(order_id)

            elif result.is_order():
                new_orders.append(result.order)
                processed_orders.append(order_id)
            else:
                raise ValueError("result must contain an order or a trade")
 
        for order_id in processed_orders:
            self.pending_orders.pop(order_id)
        for order in new_orders:
            self._add_order(order)

        if len(new_orders) > 0:
            self._process_orders(new_orders)

    def add_market_order(self, side: Side, quantity: Decimal) -> Order:
        """
        Add a market order to the broker.
        """
        order = MarketOrder(self._next_order_id, self.current_time, side, quantity)
        self._add_order(order)
        self._next_order_id += 1
        return order

    def add_limit_order(self, side: Side, quantity: Decimal, price: Decimal) -> Order:
        """
        Add a limit order to the broker.
        """
        order = LimitOrder(self._next_order_id, self.current_time, side, quantity, price)
        self._add_order(order)
        self._next_order_id += 1
        return order
    

@dataclass
class Stats:
    num_orders : int = field(default=0)
    num_positions : int = field(default=0)
    num_closed_positions : int = field(default=0)
    num_canceled_orders : int = field(default=0)
    num_executed_orders : int = field(default=0)
    num_open_orders : int = field(default=0)

@dataclass
class Strategy(metaclass=ABCMeta):
    data : DataSet
    broker_config = InitVar[VBrokerConfig]
    broker: VBroker = field(default=None, init=False)

    def __post_init__(self):
        if self.data is None:
            raise ValueError("data is required")
        if self.data.has_data() == False:
            raise ValueError("data is empty")
        self.broker = VBroker(data=self.data, config=self.broker_config)  

    def next_data(self):
        self.broker.next_data()
        self.on_next_data()
    
    @abstractmethod
    def on_next_data(self):
        pass

    @abstractmethod
    def initialize(self):
        pass

    def buy(self, type: OrderType, quantity: Decimal, price: Decimal = 0, take_profit: Decimal = 0, stop_loss: Decimal = 0) -> Order:
        return self.broker.create_order(Side.long, type, quantity, price, take_profit, stop_loss)
    
    def sell(self, type: OrderType, quantity: Decimal, price: Decimal = 0, take_profit: Decimal = 0, stop_loss: Decimal = 0) -> Order:
        return self.broker.create_order(Side.short, type, quantity, price, take_profit, stop_loss)

    def get_pending_orders(self) -> List[Order]:
        return self.broker.pending_orders
    
    def get_open_order_by_id(self, order_id: int) -> Order:
        return self.broker.pending_orders[order_id]
    
    def get_open_positions(self) -> List[Position]:
        return self.broker.open_positions
    
    

# @dataclass
# class Backtest:
#    Strategy: Strategy
#
#    def __post_init__(self):
#         self.vbroker = VBroker(data=self.dataset)
#         self.Strategy.initialize_broker(self.vbroker)
#
#    def start_backtesting(self):
#        self.Strategy.next_data()

