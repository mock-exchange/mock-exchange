import sys
import math
from collections import deque
from io import StringIO
import lmdb

from .ordertree import OrderTree

import stats

TS = stats.Stats()

class OrderBook(object):
    @TS.timeit
    def __init__(self, dbname, dbsize, tick_size = 0.0001):
        self.tape = deque(maxlen=None) # Index [0] is most recent trade
        self.lastTick = None
        self.lastTimestamp = 0
        self.tickSize = tick_size
        self.time = 0
        self.nextQuoteID = 0

        self.dbname = dbname
        self.dbsize = dbsize

        self.verbose = False

        # LMDB
        self.env = lmdb.open(str(self.dbname),
            map_size=self.dbsize, max_dbs=3)
        self.bids_db = self.env.open_db(b'bids', dupsort=True)
        self.asks_db = self.env.open_db(b'asks', dupsort=True)
        self.ids_db = self.env.open_db(b'ids')

        self.bids = OrderTree(self.env, self.bids_db, self.ids_db, 'bid')
        self.asks = OrderTree(self.env, self.asks_db, self.ids_db, 'ask')

    def clipPrice(self, price):
        """ Clips the price according to the ticksize """
        return round(price, int(math.log10(1 / self.tickSize)))

    def updateTime(self):
        self.time+=1

    @TS.timeit
    def commit(self):
        print('commit()')
        print('  write trades')
        print('  write ledgers')
        print('  write completed orders')
        print('  write book cache')
        print('  write ohlcv')

    @TS.timeit
    def processOrder(self, quote):
        quote['timestamp'] = 0
        orderType = quote['type']
        orderInBook = None
        if quote['qty'] <= 0:
            sys.exit('processLimitOrder() given order of qty <= 0')
        if orderType=='market':
            trades = self.processMarketOrder(quote)
        elif orderType=='limit':
            quote['price'] = self.clipPrice(quote['price'])
            trades, orderInBook = self.processLimitOrder(quote)
        else:
            sys.exit("processOrder() given neither 'market' nor 'limit'")
        return trades, orderInBook

    @TS.timeit
    def processList(self, side, quote, qtyStillToTrade):
        trades = []
        qtyToTrade = qtyStillToTrade
        if side == 'bid':
            ii = self.bids
        elif side == 'ask':
            ii = self.asks
        #while len(orderlist) > 0 and qtyToTrade > 0:
        print('-'*78)
        cnt = 0
        for order in ii:
            if qtyToTrade <= 0:
                break

            cnt += 1
            print(cnt, order)
            tradedPrice = order.price
            counterparty = order.tid
            if qtyToTrade < order.qty:
                tradedQty = qtyToTrade
                # Amend book order
                newBookQty = order.qty - qtyToTrade
                ii.updateQty(order, newBookQty)
                qtyToTrade = 0
            elif qtyToTrade == order.qty:
                tradedQty = qtyToTrade
                ii.removeOrder(order)
                qtyToTrade = 0
            else:
                tradedQty = order.qty
                ii.removeOrder(order)
                # We need to keep eating into volume at this price
                qtyToTrade -= tradedQty

            if self.verbose:
                print('>>> TRADE \nt=%d $%f n=%d p1=%d p2=%d' % (
                    self.time, tradedPrice, tradedQty,
                    counterparty, quote['tid']
                ))

            # Trade Transaction
            tx = {
                'timestamp' : self.time,
                'price'     : tradedPrice,
                'qty'       : tradedQty
            }
            if side == 'bid':
                tx['party1'] = [counterparty, 'bid', order.idNum]
                tx['party2'] = [quote['tid'], 'ask', None]
            else:
                tx['party1'] = [counterparty, 'ask', order.idNum]
                tx['party2'] = [quote['tid'], 'bid', None]

            self.tape.append(tx)
            trades.append(tx)

        return qtyToTrade, trades

    @TS.timeit
    def processMarketOrder(self, quote):
        trades = []
        qtyToTrade = quote['qty']
        side = quote['side']
        if side == 'bid':
            while qtyToTrade > 0 and self.asks:
                qtyToTrade, newTrades = self.processList(quote, qtyToTrade)
                trades += newTrades
        elif side == 'ask':
            while qtyToTrade > 0 and self.bids:
                qtyToTrade, newTrades = self.processList(quote, qtyToTrade)
                trades += newTrades
        else:
            sys.exit('processMarketOrder() received neither "bid" nor "ask"')
        return trades

    def getSide(self, side, other=False):
        if other: side = 'ask' if side == 'bid' else 'bid'
        obj = self.bids if side == 'bid' else self.asks
        return side, obj

    @TS.timeit
    def processLimitOrder(self, quote):
        orderInBook = None
        trades = []
        qtyToTrade = quote['qty']
        side = quote['side']
        price = quote['price']

        otherSide, orderList = self.getSide(side, True)

        orderList.initPrices()
        while (orderList and
               ((otherSide == 'ask' and price >= orderList.firstPrice()) or
               (otherSide == 'bid' and price <= orderList.firstPrice())) and
               qtyToTrade > 0):
            qtyToTrade, newTrades = self.processList(otherSide,
                quote, qtyToTrade)
            trades += newTrades
        # If volume remains, add to book
        if qtyToTrade > 0:
            quote['qty'] = qtyToTrade
            orderList.insertOrder(quote)
            orderInBook = quote

        return trades, orderInBook

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


    @TS.timeit
    def tapeDump(self, fname, fmode, tmode):
            dumpfile = open(fname, fmode)
            for tapeitem in self.tape:
                dumpfile.write('%s, %s, %s\n' % (tapeitem['timestamp'], 
                                                 tapeitem['price'], 
                                                 tapeitem['qty']))
            dumpfile.close()
            if tmode == 'wipe':
                    self.tape = []

    @TS.timeit
    def __str__(self):
        fileStr = StringIO()
        fileStr.write("------ Bids -------\n")
        if self.bids != None and len(self.bids) > 0:
            for k, v in self.bids.priceTree.items(reverse=True):
                fileStr.write('%s' % v)
        fileStr.write("\n------ Asks -------\n")
        if self.asks != None and len(self.asks) > 0:
            for k, v in self.asks.priceTree.items():
                fileStr.write('%s' % v)
        fileStr.write("\n------ Trades ------\n")
        if self.tape != None and len(self.tape) > 0:
            num = 0
            for entry in self.tape:
                if num < 5:
                    fileStr.write(str(entry['qty']) + " @ " + 
                                  str(entry['price']) + 
                                  " (" + str(entry['timestamp']) + ")\n")
                    num += 1
                else:
                    break

        fileStr.write("\n" + "="*20 + '  LMDB  ' + "="*20 + "\n")

        fileStr.write("------ Bids -------\n")
        fileStr.write(self.bids.get_db_list())

        fileStr.write("\n------ Asks -------\n")
        fileStr.write(self.asks.get_db_list())

        fileStr.write("\n------ Trades -----\n")

        fileStr.write("\n")
        return fileStr.getvalue()

