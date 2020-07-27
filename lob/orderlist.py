from sortedcontainers import SortedList, SortedSet

from lob.model import Order, encode, decode
from stats import Stats, get_size, sizefmt
import time

TS = Stats()

class OrderList:
    def __init__(self, env, side):
        self.env = env
        self.side = side

        self.desc = 'desc' if side == 'bid' else 'asc'

        self.idb = env.open_db(b'ids')

        if side == 'bid':
            self.db = env.open_db(b'bids', dupsort=True)
        elif side == 'ask':
            self.db = env.open_db(b'asks', dupsort=True)
        else:
            raise Exception('Invalid side: '+side)


        """
        Sequence key

        The sequence key is used in both the orders SortedList and lmdb
        databases bids and asks. The key is 16 bytes. 8 bytes price +
        8 bytes id (sequence number). The id (sequence number) represents
        the order of orders.

        Processing order is as follows:
        For asks, ascending price and ascending id. The first order to match
        is the lowest price and the first order at that price.

        For bids, descending price and ascending id. The first order to match
        is the highest price and the first order at that price.

        Price + id for asks are already in combined sort order. For bids,
        we change the id to a negative number so that sorting both combined
        result in proper order.

        """

        # Current batch of orders
        self.order_idx = {}          # order.id -> Order
        self.orders = SortedList()   # [sequence key..]

        # Current pending operations
        self.pending = {}            # order.id -> [ops..]

        # Orders waiting to be deleted are moved here
        self.deleted_order_idx = {}  # order.id -> Order

        self.dirty = False

        # Hydrate current batch
        self.init()

        # Debug
        print(self.side)
        print('orders:',len(self.orders),'size:',get_size(self.orders))
        print('order_idx:',len(self.order_idx.keys()), 'size:', get_size(self.order_idx))


    @TS.timeit
    def init(self):
        orders, order_idx = self.get_raw_list()
        self.order_idx = order_idx
        self.orders = SortedList(orders)

    def refill(self):
        if self.dirty:
            raise Exception('Cannot refill while dirty. flush first')
        pass
        # update self.orders
        # update self.order_idx

    def dump_pending(self):
        print("------ Pending -------")
        print(self.side+":")
        for k in sorted(self.pending.keys()):
            v = self.pending[k]
            if v[-1] == 'remove':
                o = self.deleted_order_idx[k]
            else:
                o = self.order_idx[k]

            print(("%10d %s %s" % (k, v, o)))

    # Flush changes to disk
    @TS.timeit
    def flush(self, txn):
        for order_id in self.pending.keys():
            ops = self.pending[order_id]
            if ops[-1] == 'remove':
                o = self.deleted_order_idx[order_id]
            else:
                o = self.order_idx[order_id]

            if ops[0] == 'insert' and ops[-1] == 'remove':
                pass
            elif ops[-1] == 'insert':
                self.db_insert(txn, o)
                o.in_db = True
            elif ops[-1] == 'remove' and o.in_db:
                self.db_delete(txn, o)
            elif ops[-1] == 'qty':
                self.db_update(txn, o)

        self.pending = {}
        self.deleted_order_idx = {}

    @TS.timeit
    def db_insert(self, txn, o):
        r1 = txn.put(encode(o.id), encode(o.qty), db=self.idb)
        r2 = txn.put(encode(o.price), encode(self.valueId(o)),
            db=self.db)
        if not r1 or not r2:
            raise Exception('Should we die on duplicate insert?')

    @TS.timeit
    def db_delete(self, txn, o):
        r1 = txn.delete(encode(o.price), encode(self.valueId(o)), db=self.db)
        r2 = txn.delete(encode(o.id), db=self.idb)
        if not r1 or not r2:
            self.dump_pending()
            print('r1:',r1,'r2:',r2)
            print(o)
            raise Exception('Should we die on failed delete?')

    @TS.timeit
    def db_update(self, txn, o):
        #value = encode(o.qty) + encode(o.account_id)
        r = txn.put(encode(o.id), encode(o.qty), db=self.idb)


    def __iter__(self):
        #print('get iter..')
        # prime pump
        if self.side == 'bid':
            return self.orders.irange(reverse=True)
        elif self.side == 'ask':
            return self.orders.irange()

    def __len__(self):
        print('get len..')
        # return len of primed pump here
        return 0
        #return len(self.queue)

    def get_order(self, raw):
        raw_id = abs(decode(raw[8:]))

        return self.order_idx[raw_id]

    def xx__next__(self):
        #if len(self.queue) < 10:
        #    self.queue = self.getlist()
        """
        print('next... ', end='')
        if not self.queue:
            print('StopIteration')
            raise StopIteration
        value = self.queue.pop(0)
        print('pop(0) value:',value)
        return value
        """


        k, v = self.icur.item()
        if not k or not v:
            raise StopIteration
        
        if self.side == 'bid':
            self.icur.prev()
            #k, v = self.icur.iterprev(True, True)
        elif self.side == 'ask':
            self.icur.next()
            #k, v = self.icur.iternext(True, True)


        id_num = abs(decode(v))
        qty = self.itxn.get(encode(id_num), db=self.idb)
        data = {'id':id_num, 'qty':decode(qty), 'price':decode(k)}
        return Order(**data)

    def db_iter_reset(self, cur):
        if self.side == 'bid':
            cur.last()
        elif self.side == 'ask':
            cur.first()

    def db_iter(self, cur):
        if self.side == 'bid':
            return cur.iterprev(True, True)
        elif self.side == 'ask':
            return cur.iternext(True, True)

    @TS.timeit
    def get_raw_list(self, size=40000):
        orders = []
        order_idx = {}
        with self.env.begin(db=self.db) as txn:
            cur = txn.cursor()

            self.db_iter_reset(cur)
            for cnt, (k, v) in enumerate(self.db_iter(cur)):
                if cnt > size: # This break should occur at the end of a dup
                    break
                orders.append(k + v)

            for raw in orders:
                price = decode(raw[:8])
                id_num = abs(decode(raw[8:]))
                qty = txn.get(encode(id_num), db=self.idb)
                data = {'id':id_num, 'qty':decode(qty), 'price':price,
                    'in_db':True}
                o = Order(**data)
                order_idx[id_num] = o

        return orders, order_idx

    def getlist(self):
        orders = []

        with self.env.begin(db=self.db) as txn:
            cur = txn.cursor()

            if self.side == 'bid':
                cur.last()
                raw = [(k,v) for k,v in cur.iterprev(True, True)]
            elif self.side == 'ask':
                cur.first()
                raw = [(k,v) for k,v in cur.iternext(True, True)]


            for k, v in raw:
                id_num = abs(decode(v))
                qty = txn.get(encode(id_num), db=self.idb)
                data = {'id':id_num, 'qty':decode(qty), 'price':decode(k)}
                o = Order(**data)
                orders.append(o)

        return orders

    def valueId(self, order):
        if self.side == 'bid':
            return order.id * -1
        return order.id

    def rawId(self, order):
        return encode(order.price) + encode(self.valueId())

    def add_pending(self, order, state):
        if order.id not in self.pending:
            self.pending[order.id] = []
        self.pending[order.id].append(state)

    @TS.timeit
    def update_qty(self, order, qty):
        self.order_idx[order.id].qty = qty
        self.add_pending(order, 'qty')

    @TS.timeit
    def insert(self, quote):
        order = Order(quote.to_dict())
        price_id = encode(order.price) + encode(self.valueId(order))
        self.order_idx[order.id] = order
        # If this price outside of what we have in memory, dont add it here
        self.orders.add(price_id)
        self.add_pending(order, 'insert')

    @TS.timeit
    def update(self, orderUpdate):
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
    def delete(self, order):
        price_id = encode(order.price) + encode(self.valueId(order))
        del self.order_idx[order.id]
        self.deleted_order_idx[order.id] = order
        # If this price outside of what we have in memory, dont add it here
        self.orders.remove(price_id)
        self.add_pending(order, 'remove')

