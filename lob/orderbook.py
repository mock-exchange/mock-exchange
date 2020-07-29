import sys
import math
from collections import deque
from io import StringIO
import lmdb
import time

from .orderlist import OrderList
from .model import Quote, Trade

import stats

TS = stats.Stats()

# A flush will occur every x seconds or every y pending operations.
FLUSH_TIME_SECONDS = 2
FLUSH_PENDING_COUNT = 5000

class OrderBook(object):
    def __init__(self, env, trades_file, tick_size = 0.0001):
        self.tape = deque(maxlen=None) # Index [0] is most recent trade
        self.trades_file = trades_file
        self.lastTick = None
        self.lastTimestamp = 0
        self.tickSize = tick_size
        self.time = 0
        self.nextQuoteID = 0

        self.verbose = False

        # LMDB
        self.env = env

        self.bids = OrderList(self.env, 'bid')
        self.asks = OrderList(self.env, 'ask')

        self.flush_size = FLUSH_PENDING_COUNT

        self.last_flush = time.time()
        self.count = 0

    def clipPrice(self, price):
        """ Clips the price according to the ticksize """
        return round(price, int(math.log10(1 / self.tickSize)))

    # Nanoseconds µs
    def currentTime(self):
        return int(time.time() * 1000 * 1000)


    @TS.timeit
    def flush(self):
        with self.env.begin(write=True) as txn:
            self.bids.flush(txn)
            self.asks.flush(txn)
            self.tapeDump(self.trades_file, 'a', 'wipe')
            #print('sleep 5 seconds after flush()..')
            #time.sleep(5)
            # write out trades
            # write out order update logs (status and qty change)
            # write out book cache (for charts)

            # I think subsequent trade processing can do these:
            # write out ledgers (do this here?)
            # write out ohlcv? (can trades produce this?)

    def getSide(self, side, other=False):
        if other: side = 'ask' if side == 'bid' else 'bid'
        obj = self.bids if side == 'bid' else self.asks
        return side, obj

    @TS.timeit
    def processOrder(self, quote):
        print('process:',quote)
        orderInBook = None
        self.count += 1
        if quote.type == 'market':
            trades = self.processMarketOrder(quote)
        elif quote.type == 'limit':
            trades, orderInBook = self.processLimitOrder(quote)
        else:
            sys.exit("processOrder() given neither 'market' nor 'limit'")

        if self.count > self.flush_size or time.time() - self.last_flush > 1:
            self.flush()
            self.last_flush = time.time()
            self.count = 0

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

        return trades, orderInBook

    def processList(self, olist, quote, qtyAss):
        qtyToTrade = qtyAss
        trades = []
        #print('processList', '-'*50)
        cnt = 0
        is_limit = quote.type == 'limit'

        # skip this loop if price <> last
        for i, seq_key in enumerate(olist):
            o = olist.get_order(seq_key)
            if qtyToTrade <= 0:
                break
            if is_limit and olist.side == 'ask' and o.price > quote.price:
                break
            elif is_limit and olist.side == 'bid' and o.price < quote.price:
                break

            print('  %-4d %s' % (i,o))

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

            # Trade Transaction
            tx = {
                'time'  : self.currentTime(),
                'price' : tradedPrice,
                'qty'   : tradedQty
            }
            if quote.side == 'bid':
                tx['party1'] = [counterparty, 'bid', o.id]
                tx['party2'] = [quote.id, 'ask', None]
            elif quote.side == 'ask':
                tx['party1'] = [counterparty, 'ask', o.id]
                tx['party2'] = [quote.id, 'bid', None]

            self.tape.append(tx)
            trades.append(tx)

        olist.apply_deletes()
        return qtyToTrade, trades


    def cancelOrder(self, side, idNum, time = None):
        if time:
            self.time = time
        else:
            self.updateTime()
        if side == 'bid':
            if self.bids.orderExists(idNum):
                self.bids.removeOrder(idNum)
        elif side == 'ask':
            if self.asks.orderExists(idNum):
                self.asks.removeOrder(idNum)
        else:
            sys.exit('cancelOrder() given neither bid nor ask')

    def modifyOrder(self, idNum, orderUpdate, time=None):
        if time:
            self.time = time
        else:
            self.updateTime()
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


    def tapeDump(self, fname, fmode, tmode):
            dumpfile = open(fname, fmode)
            for tapeitem in self.tape:
                dumpfile.write('%s,%s,%s\n' % (tapeitem['time'],
                                                 tapeitem['price'],
                                                 tapeitem['qty']))
            dumpfile.close()
            if tmode == 'wipe':
                self.tape = deque(maxlen=None)

    def __str__(self):
        fileStr = StringIO()
        
        """
        fileStr.write("\n" + "="*20 + '  LMDB  ' + "="*20 + "\n")

        fileStr.write("------ Bids -------\n")
        for o, null in self.bids.get_raw_list(-1):
            add = ("%10d @ %8.2f %10d\n" % (
                o.qty, o.price, o.id,
            ))
            fileStr.write(add)

        fileStr.write("\n------ Asks -------\n")
        for o, null in self.asks.get_raw_list(-1):
            add = ("%10d @ %8.2f %10d\n" % (
                o.qty, o.price, o.id,
            ))
            fileStr.write(add)
        """
        fileStr.write("\n------ Trades -----\n")
        if self.tape != None and len(self.tape) > 0:
            num = 0
            for entry in self.tape:
                if num < 5:
                    fileStr.write(str(entry['qty']) + " @ " + 
                                  str(entry['price']) + 
                                  " (" + str(entry['time']) + ")\n")
                    num += 1
                else:
                    break

        """
        fileStr.write("\n------ Pending -------\n")
        fileStr.write("bids:\n")
        for k, v in self.bids.pending.items():
            add = ("%10d %s\n" % (k,v))
            fileStr.write(add)
        fileStr.write("\nasks:\n")
        for k, v in self.asks.pending.items():
            add = ("%10d %s\n" % (k,v))
            fileStr.write(add)
        """

        fileStr.write("\n")
        return fileStr.getvalue()

