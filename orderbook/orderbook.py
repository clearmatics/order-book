# type: ignore

from six.moves import cStringIO as StringIO
from decimal import Decimal

from .ordertree import OrderTree


class OrderBookError(Exception):
    pass


class OrderBook(object):
    def __init__(self):
        self.bids = OrderTree()
        self.asks = OrderTree()
        self.time = 0

    def process_order(self, quote, verbose):
        order_type = quote["type"]
        order_in_book = None
        self.time = quote["timestamp"]
        if quote["quantity"] <= 0:
            raise OrderBookError("process_order() given order of quantity <= 0")
        if order_type == "market":
            trades = self.process_market_order(quote, verbose)
        elif order_type == "limit":
            quote["price"] = Decimal(quote["price"])
            trades, order_in_book = self.process_limit_order(quote, verbose)
        else:
            raise OrderBookError("order_type for process_order() is neither 'market' or 'limit'")
        return trades, order_in_book

    def process_order_list(
        self, side, order_list, quantity_still_to_trade, quote, verbose
    ):
        """
        Takes an OrderList (stack of orders at one price) and an incoming order and matches
        appropriate trades given the order's quantity.
        """
        trades = []
        quantity_to_trade = quantity_still_to_trade
        while len(order_list) > 0 and quantity_to_trade > 0:
            head_order = order_list.get_head_order()
            traded_price = head_order.price
            counter_party = head_order.trade_id
            new_book_quantity = None
            if quantity_to_trade < head_order.quantity:
                traded_quantity = quantity_to_trade
                # Do the transaction
                new_book_quantity = head_order.quantity - quantity_to_trade
                head_order.update_quantity(new_book_quantity, head_order.timestamp)
                quantity_to_trade = 0
            elif quantity_to_trade == head_order.quantity:
                traded_quantity = quantity_to_trade
                if side == "bid":
                    self.bids.remove_order_by_id(head_order.order_id)
                else:
                    self.asks.remove_order_by_id(head_order.order_id)
                quantity_to_trade = 0
            else:  # quantity to trade is larger than the head order
                traded_quantity = head_order.quantity
                if side == "bid":
                    self.bids.remove_order_by_id(head_order.order_id)
                else:
                    self.asks.remove_order_by_id(head_order.order_id)
                quantity_to_trade -= traded_quantity
            if verbose:
                print(
                    (
                        "TRADE: Time - {}, Price - {}, Quantity - {}, TradeID - {}, Matching TradeID - {}".format(
                            self.time,
                            traded_price,
                            traded_quantity,
                            counter_party,
                            quote["trade_id"],
                        )
                    )
                )

            transaction_record = {
                "timestamp": self.time,
                "price": traded_price,
                "quantity": traded_quantity,
                "time": self.time,
            }

            if side == "bid":
                transaction_record["party1"] = [
                    counter_party,
                    "bid",
                    head_order.order_id,
                    new_book_quantity,
                ]
                transaction_record["party2"] = [quote["trade_id"], "ask", None, None]
            else:
                transaction_record["party1"] = [
                    counter_party,
                    "ask",
                    head_order.order_id,
                    new_book_quantity,
                ]
                transaction_record["party2"] = [quote["trade_id"], "bid", None, None]

            trades.append(transaction_record)
        return quantity_to_trade, trades

    def process_market_order(self, quote, verbose):
        trades = []
        quantity_to_trade = quote["quantity"]
        side = quote["side"]
        if side == "bid":
            while quantity_to_trade > 0 and self.asks:
                best_price_asks = self.asks.min_price_list()
                quantity_to_trade, new_trades = self.process_order_list(
                    "ask", best_price_asks, quantity_to_trade, quote, verbose
                )
                trades += new_trades
        elif side == "ask":
            while quantity_to_trade > 0 and self.bids:
                best_price_bids = self.bids.max_price_list()
                quantity_to_trade, new_trades = self.process_order_list(
                    "bid", best_price_bids, quantity_to_trade, quote, verbose
                )
                trades += new_trades
        else:
            raise OrderBookError('process_market_order() recieved neither "bid" nor "ask"')
        return trades

    def process_limit_order(self, quote, verbose):
        order_in_book = None
        trades = []
        quantity_to_trade = quote["quantity"]
        side = quote["side"]
        price = quote["price"]
        if side == "bid":
            while (
                self.asks and price >= self.asks.min_price() and quantity_to_trade > 0
            ):
                best_price_asks = self.asks.min_price_list()
                quantity_to_trade, new_trades = self.process_order_list(
                    "ask", best_price_asks, quantity_to_trade, quote, verbose
                )
                trades += new_trades
            # If volume remains, need to update the book with new quantity
            if quantity_to_trade > 0:
                quote["quantity"] = quantity_to_trade
                self.bids.insert_order(quote)
                order_in_book = quote
        elif side == "ask":
            while (
                self.bids and price <= self.bids.max_price() and quantity_to_trade > 0
            ):
                best_price_bids = self.bids.max_price_list()
                quantity_to_trade, new_trades = self.process_order_list(
                    "bid", best_price_bids, quantity_to_trade, quote, verbose
                )
                trades += new_trades
            # If volume remains, need to update the book with new quantity
            if quantity_to_trade > 0:
                quote["quantity"] = quantity_to_trade
                self.asks.insert_order(quote)
                order_in_book = quote
        else:
            raise OrderBookError('process_limit_order() given neither "bid" nor "ask"')
        return trades, order_in_book

    def cancel_order(self, side, order_id):
        if side == "bid":
            if self.bids.order_exists(order_id):
                self.bids.remove_order_by_id(order_id)
                return True
            return False
        elif side == "ask":
            if self.asks.order_exists(order_id):
                self.asks.remove_order_by_id(order_id)
                return True
            return False
        else:
            raise OrderBookError('cancel_order() given neither "bid" nor "ask"')

    def get_volume_at_price(self, side, price):
        price = Decimal(price)
        if side == "bid":
            volume = 0
            if self.bids.price_exists(price):
                volume = self.bids.get_price_list(price).volume
            return volume
        elif side == "ask":
            volume = 0
            if self.asks.price_exists(price):
                volume = self.asks.get_price_list(price).volume
            return volume
        else:
            raise OrderBookError('get_volume_at_price() given neither "bid" nor "ask"')

    def get_best_bid(self):
        return self.bids.max_price()

    def get_worst_bid(self):
        return self.bids.min_price()

    def get_best_ask(self):
        return self.asks.min_price()

    def get_worst_ask(self):
        return self.asks.max_price()

    def __str__(self):
        tempfile = StringIO()
        tempfile.write("***Bids***\n")
        if self.bids is not None and len(self.bids) > 0:
            for _, value in reversed(self.bids.price_map.items()):
                tempfile.write("%s" % value)
        tempfile.write("\n***Asks***\n")
        if self.asks is not None and len(self.asks) > 0:
            for _, value in self.asks.price_map.items():
                tempfile.write("%s" % value)
        return tempfile.getvalue()
