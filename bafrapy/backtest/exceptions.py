from dataclasses import dataclass

@dataclass
class BrokerException(Exception):
    pass


@dataclass
class OrderAlreadyExists(BrokerException):
    order_id : int

    def __str__(self):
        return f"OrderAlreadyExists: order with {self.order_id} id already exists"


@dataclass
class NotEnoughMoney(BrokerException):
    
    def __str__(self):
        return f"NotEnoughMoney: broker has not enough money"
    
@dataclass
class NotEnoughMoneyToExecuteMarketOrder(BrokerException):
    order_id : int

    def __str__(self):
        return f"""
            NotEnoughMoneyToExecute: not enough money to execute buy market order order with id {self.order_id}.

        """

@dataclass
class NotEnoughQuoteToExecuteMarketOrder(BrokerException):
    order_id : int

    def __str__(self):
        return f"""
            NotEnoughMoneyToExecute: not enough money to execute sell market order order with id {self.order_id}
        """

@dataclass
class NotEnoughQuote(BrokerException):
    
    def __str__(self):
        return f"NotEnoughQuote: broker has not enough quote"
    

@dataclass
class OrderException(Exception):
    pass

@dataclass
class NewOrderNotOpen(OrderException):
    order_id : int

    def __str__(self):
        return f"NewOrderNotOpen: order with {self.order_id} id is not open"

@dataclass
class UncancellableOrder(OrderException):
    order_id : int

    def __str__(self):
        return f"UncancellableOrder: order with {self.order_id} id is not cancelable"

@dataclass
class InvalidStateExecutedSimpleOrder(OrderException):
    order_id : int

    def __str__(self):
        return f"InvalidStateExecutedSimpleOrder: order with {self.order_id} was executed but its state is invalid"

@dataclass
class InvalidStateExecutedCompositeOrder(OrderException):
    order_id : int

    def __str__(self):
        return f"InvalidStateExecutedCompositeOrder: order with {self.order_id} was executed but its state is invalid"