
class BrokerException(Exception):
    pass


class OrderAlreadyExists(BrokerException):
    def __init__(self, order_id: int):
        self.order_id = order_id

    def __str__(self):
        return f"OrderAlreadyExists: order with {self.order_id} id already exists"


class NotEnoughMoney(BrokerException):
    def __str__(self):
        return "NotEnoughMoney: broker has not enough money"
  
    
class NotEnoughMoneyToExecuteMarketOrder(BrokerException):
    def __init__(self, order_id: int):
        self.order_id = order_id

    def __str__(self):
        return f"""
            NotEnoughMoneyToExecute: not enough money to execute buy market order order with id {self.order_id}.

        """


class NotEnoughQuoteToExecuteMarketOrder(BrokerException):
    def __init__(self, order_id: int):
        self.order_id = order_id

    def __str__(self):
        return f"""
            NotEnoughMoneyToExecute: not enough money to execute sell market order order with id {self.order_id}
        """


class NotEnoughQuote(BrokerException):
    def __str__(self):
        return "NotEnoughQuote: broker has not enough quote"
    

class OrderException(Exception):
    pass


class NewOrderNotOpen(OrderException):
    def __init__(self, order_id: int):
        self.order_id = order_id

    def __str__(self):
        return f"NewOrderNotOpen: order with {self.order_id} id is not open"


class UncancellableOrder(OrderException):
    def __init__(self, order_id: int):
        self.order_id = order_id

    def __str__(self):
        return f"UncancellableOrder: order with {self.order_id} id is not cancelable"


class InvalidStateExecutedSimpleOrder(OrderException):
    def __init__(self, order_id: int):
        self.order_id = order_id

    def __str__(self):
        return f"InvalidStateExecutedSimpleOrder: order with {self.order_id} was executed but its state is invalid"


class InvalidStateExecutedCompositeOrder(OrderException):
    def __init__(self, order_id: int):
        self.order_id = order_id

    def __str__(self):
        return f"InvalidStateExecutedCompositeOrder: order with {self.order_id} was executed but its state is invalid"