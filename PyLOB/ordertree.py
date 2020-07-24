from io import StringIO
import sys

import stats

TS = stats.Stats()


def encode(i): return int(i).to_bytes(8, 'big')
def decode(v): return int.from_bytes(v, 'big')

# Pack even sized field
def pack(v): return b''.join(v)
def unpack(f, s=8): return [f[i*s:(i*s)+s] for i in range(int(len(f) / s))]
# id,qty,price,account_id

# bids:
# price, pack(id, qty, account_id)

fsizes = (8,4,4,4)

class Order(object):
    def __init__(self, quote, orderList):
        self.timestamp = int(quote['timestamp'])
        self.qty = int(quote['qty'])
        self.price = quote['price']
        self.idNum = quote['idNum']
        self.tid = quote['tid']
        #self.nextOrder = None
        #self.prevOrder = None
        #self.orderList = orderList

    def __str__(self):
        #return "%s\t@\t%.4f\tt=%d" % (self.qty, self.price, self.timestamp)
        return "%-10d @ %10.2f  tid:%-6d" % (self.qty, self.price, self.tid)


class OrderTree(object):
    def __init__(self, env, db, idb, side):
        self.priceTree = {}
        self.priceMap = {}  # Map from price -> orderList object
        self.orderMap = {}  # Order ID to Order object
        self.volume = 0     # How much volume on this side?
        self.nOrders = 0   # How many orders?
        self.lobDepth = 0  # How many different prices on lob?

        self.__prices = []
        self._shits = []

        self.env = env
        self.db = db
        self.idb = idb
        self.side = side

        self.txn = self.env.begin(db=self.db)

    def __iter__(self):

        return self

    def __next__(self):
        if len(self._shits) < 1:
            raise StopIteration
        return self._shits.pop(0)


    def subc(self, txn, cur2, key):
        #cur2 = txn.cursor()
        cur2.set_key(key)
        last_value = 0
        back = []
        for key, value in cur2.iternext_dup(True, True):
            this_value = decode(value)
            flag = ''
            if this_value <= last_value:
                flag = '* sort error'
                #global SORT_ERRORS
                #SORT_ERRORS += 1
            last_value = this_value

            tid = this_value
            price = decode(key)
            qqq = txn.get(value, db=self.idb)

            qty = decode(qqq)
            f = "%-10d @ %10.2f  tid:%-6d  %s" % (qty, price, tid, flag)

            back.append(f)
        return back

    def get_db_list(self):
        txn = self.env.begin(db=self.db)
        cur = txn.cursor()

        keys = []
        if self.side == 'ask':
            cur.first()
            for key in cur.iternext_nodup(True):
                keys.append(key)
        elif self.side == 'bid':
            cur.last()
            for key in cur.iterprev_nodup(True):
                keys.append(key)

        bitches = []
        for key in keys:
            #print(decode(key))
            back = self.subc(txn, cur, key)
            bitches.extend(back)

        return "\n".join(bitches) + "\n"

    @TS.timeit
    def initPrices(self):
        txn = self.env.begin(db=self.db)
        cur = txn.cursor()

        keys = []
        if self.side == 'ask':
            cur.first()
            for key in cur.iternext_nodup(True):
                keys.append(key)
        elif self.side == 'bid':
            cur.last()
            for key in cur.iterprev_nodup(True):
                keys.append(key)
        self.__prices = keys


        self._shits = []

        if len(self.__prices) <= 0:
            return
        cur.set_key(self.__prices[0])

        for key, value in cur.iternext_dup(True, True):
            tid = decode(value)
            price = decode(key)
            qqq = txn.get(value, db=self.idb)
            qty = decode(qqq)
            #print(">> %-10d @ %10.2f  tid:%-6d" % (qty, price, tid))

            quote = {
                'timestamp': 1,
                'qty': qty,
                'price': price,
                'tid': tid,
                'idNum': tid
            }
            o = Order(quote, None)
            self._shits.append(o)

    @TS.timeit
    def updateQty(self, order, qty):
        txn = self.env.begin(write=True)
        txn.put(encode(order.tid), encode(order.qty), db=self.idb)
        txn.commit()

    def __len__(self):
        return len(self._shits)
        #return len(self.orderMap)

    def getPrice(self, price):
        return self.priceMap[price]

    def getOrder(self, idNum):
        return self.orderMap[idNum]

    def createPrice(self, price):
        self.lobDepth += 1
        newList = OrderList()
        self.priceTree.insert(price, newList)
        self.priceMap[price] = newList

    def removePrice(self, price):
        self.lobDepth -= 1
        self.priceTree.remove(price)
        del self.priceMap[price]

    def priceExists(self, price):
        return price in self.priceMap

    def orderExists(self, idNum):
        return idNum in self.orderMap

    @TS.timeit
    def insertOrder(self, quote):
        """
        if self.orderExists(quote['idNum']):
            self.removeOrderById(quote['idNum'])
        self.nOrders += 1
        if quote['price'] not in self.priceMap:
            self.createPrice(quote['price'])
        order = Order(quote, self.priceMap[quote['price']])
        """
        self.nOrders += 1

        order = Order(quote, None)
        #self.priceMap[order.price].appendOrder(order)
        #self.orderMap[order.idNum] = order
        self.volume += order.qty

        txn = self.env.begin(write=True)
        txn.put(encode(order.tid), encode(order.qty), db=self.idb)
        txn.put(encode(order.price), encode(order.tid), db=self.db)
        txn.commit()

    def updateOrder(self, orderUpdate):
        order = self.orderMap[orderUpdate['idNum']]
        originalVolume = order.qty
        if orderUpdate['price'] != order.price:
            # Price changed
            orderList = self.priceMap[order.price]
            orderList.removeOrder(order)
            if len(orderList) == 0:
                self.removePrice(order.price)
            self.insertOrder(orderUpdate)
        else:
            # Quantity changed
            order.updateQty(orderUpdate['qty'], orderUpdate['timestamp'])
        self.volume += order.qty-originalVolume

    @TS.timeit
    def removeOrder(self, order):
        self.nOrders -= 1
        txn = self.env.begin(write=True)
        txn.delete(encode(order.price), encode(order.tid), db=self.db)
        txn.delete(encode(order.tid), db=self.idb)
        txn.commit()

    @TS.timeit
    def firstPrice(self):
        if len(self.__prices) > 0:
            return decode(self.__prices[0])
        else:
            return None


