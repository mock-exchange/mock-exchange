import os
import sys
import math
from collections import deque
from io import StringIO
import lmdb
from time import time
from stats import get_size, sizefmt

from .orderlist import OrderList
from .model import Quote, Trade, decode


FLUSH_TIME  = 1      # Number of seconds until flush()
FLUSH_COUNT = 20000  # Number of orders until flush()

class OrderBook(object):
    def __init__(self, env, trades_dir):
        self.tape = deque(maxlen=None) # Index [0] is most recent trade
        self.trades_dir = trades_dir

        self.verbose = False

        # LMDB
        self.env = env

        self.bids = OrderList(self.env, 'bid')
        self.asks = OrderList(self.env, 'ask')

        # Since last flush
        self.flushed = time()
        self.count = 0

        #self.ocnt = 0
        #self.ccnt = 0

        self.history = []

    # self.count('orders')
    # self.count('cancels')
    #@property
    #def count(self, name):
    #    pass

    # Nanoseconds µs
    def time_ns(self):
        return int(time() * 1000 * 1000)

    def check_flush(self):
        elapsed = time() - self.flushed
        if (self.count > FLUSH_COUNT or elapsed > FLUSH_TIME):

            if len(self.history) > 10:
                n = len(self.history) - 10
                #self.history = self.history[n:]
                self.history.pop(0)
            self.history.append((self.count, elapsed))

            self.flush()
            self.flushed = time()
            self.count = 0

    def flush(self):
        with self.env.begin(write=True) as txn:
            self.bids.flush(txn)
            self.asks.flush(txn)
            self.flush_trades()
            #print('sleep 5 seconds after flush()..')
            #time.sleep(5)
            # write out trades
            # write out order update logs (status and qty change)
            # write out book cache (for charts)

            # I think subsequent trade processing can do these:
            # write out ledgers (do this here?)
            # write out ohlcv? (can trades produce this?)

    def flush_trades(self):
        keys = (
            'time','price','qty','taker_side',
            'maker_order_id','maker_account_id',
            'taker_order_id','taker_account_id'
        )

        if not self.tape:
            return
        if not os.path.exists(self.trades_dir):
            os.mkdir(self.trades_dir)
        tmpfile = self.trades_dir / '.tmp'
        permfile = self.trades_dir / str(self.time_ns())
        with open(tmpfile, 'w') as f:
            for t in self.tape:
                f.write(",".join([str(t[x]) for x in keys]) + "\n")

        os.rename(tmpfile, permfile)
        self.tape = deque(maxlen=None)

    def dump_history(self):
        for i in range(len(self.history)):
            count, elapsed = self.history[i]
            print("%-5d %6d %6.2f %8d ops/sec" % (
                i, count, elapsed, count / elapsed))


    def processOrder(self, quote):
        orderInBook = None
        self.count += 1
        if quote.type == 'market':
            trades = self.processMarketOrder(quote)
        elif quote.type == 'limit':
            trades, orderInBook = self.processLimitOrder(quote)
        else:
            sys.exit("processOrder() given neither 'market' nor 'limit'")

        #self.check_flush()

        return trades, orderInBook

    def processMarketOrder(self, quote):
        trades = []
        qtyToTrade = quote.qty
        if quote.side == 'bid':
            olist = self.asks
        elif quote.side == 'ask':
            olist = self.bids
        qtyToTrade, newTrades = self.processList(olist, quote, qtyToTrade)
        trades += newTrades
        return trades

    def processLimitOrder(self, quote):
        orderInBook = None
        trades = []

        qtyToTrade = quote.qty
        # Other side
        if quote.side == 'bid':
            olist = self.asks
        elif quote.side == 'ask':
            olist = self.bids
        qtyToTrade, newTrades = self.processList(olist, quote, qtyToTrade)
        trades += newTrades

        # If volume remains, add to book
        if qtyToTrade > 0:
            quote.qty = qtyToTrade
            # This side
            if quote.side == 'bid':
                tlist = self.bids
            elif quote.side == 'ask':
                tlist = self.asks
            tlist.insert(quote)
            orderInBook = quote

            # Book Cache Transaction
            #booktx = [quote.side, quote.price, quote.qty]

        return trades, orderInBook

    def processList(self, olist, quote, qtyAss):
        qtyToTrade = qtyAss
        trades = []
        #print('processList', '-'*50)
        cnt = 0
        is_limit = quote.type == 'limit'

        for i, seq_key in enumerate(olist):
            o = olist.get_order(seq_key)
            if qtyToTrade <= 0:
                break
            if is_limit and olist.side == 'ask' and o.price > quote.price:
                break
            elif is_limit and olist.side == 'bid' and o.price < quote.price:
                break

            #foo = '  %-4d %s' % (i,o)
            #foo = ','.join((str(o.price),str(o.id)))
            #foo = "%d,%d" % (o.price,o.id)

            cnt += 1
            #print(cnt, o)
            tradedPrice = o.price
            counterparty = o.id
            if qtyToTrade < o.qty:
                tradedQty = qtyToTrade
                # Amend book order
                newBookQty = o.qty - qtyToTrade
                olist.update_qty(o, newBookQty)
                qtyToTrade = 0
            elif qtyToTrade == o.qty:
                tradedQty = qtyToTrade
                olist.delete(o)
                qtyToTrade = 0
            else:
                tradedQty = o.qty
                olist.delete(o)
                # We need to keep eating into volume at this price
                qtyToTrade -= tradedQty

            if self.verbose:
                print('TRADE qty:%d @ $%.2f   p1=%d p2=%d  (left:%d)' % (
                    tradedQty, tradedPrice,
                    counterparty, quote.id, qtyToTrade
                ))

            # Book Cache Transaction
            #booktx = [olist.side, o.price, tradedQty * -1]

            # Trade Transaction
            tx = {
                'time'  : self.time_ns(),
                'price' : tradedPrice,
                'qty'   : tradedQty,
                # maker is order, taker is quote
                #'maker': [olist.side, o.id],
                #'taker': [quote.side, quote.id],
                'maker_order_id'   : o.id,
                'maker_account_id' : o.account_id,
                'taker_order_id'   : quote.id,
                'taker_account_id' : quote.account_id,
                'taker_side' : quote.side
            }

            self.tape.append(tx)
            trades.append(tx)

        olist.apply_deletes()
        return qtyToTrade, trades


    # need: side, price, id
    def cancelOrder(self, side, idNum):
        if side == 'bid':
            if self.bids.orderExists(idNum):
                self.bids.removeOrder(idNum)
        elif side == 'ask':
            if self.asks.orderExists(idNum):
                self.asks.removeOrder(idNum)
        else:
            sys.exit('cancelOrder() given neither bid nor ask')

    def modifyOrder(self, idNum, orderUpdate):
        side = orderUpdate['side']
        orderUpdate['idNum'] = idNum
        orderUpdate['timestamp'] = self.time
        if side == 'bid':
            if self.bids.orderExists(orderUpdate['idNum']):
                self.bids.updateOrder(orderUpdate)
        elif side == 'ask':
            if self.asks.orderExists(orderUpdate['idNum']):
                self.asks.updateOrder(orderUpdate)
        else:
            sys.exit('modifyOrder() given neither bid nor ask')

    def getVolumeAtPrice(self, side, price):
        price = self.clipPrice(price)
        if side =='bid':
            vol = 0
            if self.bids.priceExists(price):
                vol = self.bids.getPrice(price).volume
            return vol
        elif side == 'ask':
            vol = 0
            if self.asks.priceExists(price):
                vol = self.asks.getPrice(price).volume
            return vol
        else:
            sys.exit('getVolumeAtPrice() given neither bid nor ask')


    def dump_book(self):
        s1 = time()
        self.bids.dump_book()
        self.asks.dump_book()
        print("%.2f ms elapsed." % ((time() - s1) * 1000,))

    def __str__(self):
        return str(self)
