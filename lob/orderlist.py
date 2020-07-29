from sortedcontainers import SortedList, SortedSet

from lob.model import Order, encode, decode
from stats import Stats, get_size, sizefmt
import time
import sys

TS = Stats()

# This the number of orders that will be held in memory. It isn't a hard
# limit as the cutoff is on the price boundary. 
ORDER_LIMIT = 5000

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

        self.iter_deletes = []
        self.iter_idx = 0

        self.dirty = False

        # Hydrate current batch
        self.init()

        # Debug
        print(self.side)
        print('orders:',len(self.orders),'size:',get_size(self.orders))
        print('order_idx:',len(self.order_idx.keys()), 'size:', get_size(self.order_idx))


    @TS.timeit
    def init(self):
        orders, order_idx = self.db_get_list()
        self.order_idx = order_idx
        self.orders = SortedList(orders)

    def dump_pending(self):
        return
        print("------ Pending -------")
        print(self.side+":")
        for k in sorted(self.pending.keys()):
            v = self.pending[k]
            if k in self.deleted_order_idx:
                o = self.deleted_order_idx[k]
            else:
                o = self.order_idx[k]
            print(("%10d %s %s" % (k, v, o)))


    # Flush changes to disk
    @TS.timeit
    def flush(self, txn):
        print('-'*70)
        self.dump_pending()
        print('flush %3s orders:%8d' % (
            self.side, len(self.orders)))
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


    def __iter__(self):
        if len(self.iter_deletes):
            raise Exception('Deletes must be applied before iterating again.')

        self.iter_idx = 0
        return self

    # When we refill, orders and db cannot be out of sync. If there are
    # pending orders, those need to be flushed first. Actually, only the
    # last order needs to be flushed. This situation occurs when the book is
    # new and when an order eats up orders. The latter situation will 
    # nearly always have pending qty updates. Becauses flushes don't occur
    # upon every new order, whenever orders is eaten up, we'll flush
    # to be safe. We'll need to track how often this occurs.

    def __next__(self):
        idx = self.iter_idx

        # The current position is beyond orders. Attempt to refill
        if idx > len(self.orders) - 1:
            self.refill()

        if len(self.orders) == 0 or idx > len(self.orders) - 1:
            raise StopIteration

        self.iter_idx += 1
        return self.orders[idx]

    def __len__(self):
        return len(self.orders)

    def refill(self):

        # Hmm.. if we flush here, trades in OrderBook are still sitting in
        # memory. If crash occurs between this and that flush, it will leave
        # an inconsistent state.
        # Maybe we can refill without flushing. If we only need the last
        # one, and the last one is an insert, we know that didn't result
        # in a trade.
        #with self.env.begin(write=True) as txn:
        #    self.flush(txn)

        end_order = None
        if len(self.orders) > 0:
            seq_key = self.orders[-1]
            order_id = decode(seq_key[8:])
            end_order = self.order_idx[order_id]

            """
            # If the last one is pending, flush it.  Do we really need this?
            if order_id in self.pending:
                ops = self.pending[order_id]
                o = self.order_idx[order_id]

                if ops[-1] == 'remove':
                    pass
                elif ops[-1] == 'insert':
                    self.db_insert(txn, o)
                    o.in_db = True
                elif ops[-1] == 'qty':
                    self.db_update(txn, o)
                    o.in_db = True
            """

        shit = None
        if end_order:
            shit = end_order.id

        print('refill() side:',self.side,' end_order:',shit,'idx:',self.iter_idx)
        orders, order_idx = self.db_get_list(order=end_order)
        self.orders += orders
        self.order_idx.update(order_idx)
        print('refill() added',len(orders),'orders. total:',len(self.orders))
        """
        if len(self.orders) > 0:
            # This does a SortedList sort. Because the stuff coming in is
            # already sorted, this is unnecessary. The self.orders SortedList
            # is used for in memory inserts.
            self.orders += orders
            self.order_idx.update(order_idx)
        else:
            self.order_idx = order_idx
            self.orders = SortedList(orders)
        """

    def get_order(self, seq_key):
        order_id = decode(seq_key[8:])
        return self.order_idx[order_id]


    @TS.timeit
    def db_insert(self, txn, o):
        r1 = txn.put(encode(o.id), encode(o.qty), db=self.idb)

        seq_key = self.seq_key(o)
        r2 = txn.put(seq_key[:8], seq_key[8:], db=self.db)
        #r2 = txn.put(encode(o.price), encode(self.valueId(o)),
        #    db=self.db)
        if not r1 or not r2:
            raise Exception('Should we die on duplicate insert?')

    @TS.timeit
    def db_delete(self, txn, o):
        seq_key = self.seq_key(o)
        r1 = txn.delete(seq_key[:8], seq_key[8:], db=self.db)

        #r1 = txn.delete(encode(o.price), encode(self.valueId(o)), db=self.db)
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

    def db_iter(self, cur):
        return cur.iternext(True, True)

        """
        if self.side == 'bid':
            return cur.iterprev(True, True)
        elif self.side == 'ask':
            return cur.iternext(True, True)
        """

    def db_iter_set(self, cur, seq_key=None):
        if seq_key:
            res1 = cur.set_key_dup(seq_key[:8], seq_key[8:])
            if not res1:
                raise Exception('db_iter_set: Failed to set_key_dup')
            """
            if self.side == 'bid':
                res2 = cur.prev_dup()
            elif self.side == 'ask':
                res2 = cur.next_dup()
            """
            if not cur.next_dup():
                return False
            return True
        else:
            if not cur.first():
                return False
            return True
        """
        elif self.side == 'bid':
            cur.last()
        elif self.side == 'ask':
            cur.first()
        """
    @TS.timeit
    def db_get_list(self, size=40000, order=None):
        orders = []
        order_idx = {}
        with self.env.begin(db=self.db) as txn:
            cur = txn.cursor()

            # If order is supplied, the cursor starts at the next one.
            seq_key = None
            if order:
                seq_key = self.seq_key(order)

            # If not, is empty
            if not self.db_iter_set(cur, seq_key):
                return orders, order_idx
            for cnt, (k, v) in enumerate(self.db_iter(cur)):
                # Break on size limit
                if size != -1 and cnt > size:
                    break
                orders.append(k + v)

            for seq_key in orders:
                price = abs(decode(seq_key[:8]))
                id_num = decode(seq_key[8:])
                qty = txn.get(encode(id_num), db=self.idb)
                data = {'id':id_num, 'qty':decode(qty), 'price':price,
                    'in_db':True}
                o = Order(**data)
                order_idx[id_num] = o

        return orders, order_idx


    def priceSeq(self, order):
        if self.side == 'bid':
            return order.price * -1
        return order.price

    def seq_key(self, order):
        return encode(self.priceSeq(order)) + encode(order.id)

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
       
        seq_key = self.seq_key(order)

        # Special rule: empty book, write immediately
        # crap. this will affect iter idx.. wait no.. insert doesnt
        # occur in iter
        if len(self.orders) == 0:
            self.orders.add(seq_key)
            self.order_idx[order.id] = order

            with self.env.begin(write=True) as txn:
                self.db_insert(txn, order)
                order.in_db = True
            print('SPECIAL INSERT',order)
            return



        self.order_idx[order.id] = order
        self.add_pending(order, 'insert')
        # ENFORCE LIMIT. If new order keep getting add within the order set,
        # it will grow indefinitely if trades are eating up the orders faster.
        # Because of this, we bisect? and truncate the list here.
        # If len(self.orders) > ORDER_LIMIT
        #   num_to_cut = len(self.orders) - ORDER_LIMIT
        #   find a price break above num_to_cut

        # If we never insert outside of orders set, do we need to price break?

        # Must insert if within the orders set (< last one)
        # Must NOT insert if outside of the orders set because it will likely
        # not be the next seq in line.

        # asks: 5,6,7   seq < [-1]
        # bids: 7,6,5   seq > [-1]
        added = 'no'
        if ((
                len(self.orders) > 0 and
                self.side == 'ask' and seq_key < self.orders[-1])
            or (
                len(self.orders) > 0 and
                self.side == 'bid' and seq_key < self.orders[-1]
            )):
                self.orders.add(seq_key)
                added = 'yes'

        #print('%'*70)
        #print('insert() added:',order.id,'to orders:',added, ' total:',len(self.orders))
        #print(self.side,':', [str(abs(decode(o[:8]))) + '-' + str(decode(o[8:])) for o in self.orders])
        #self.dump_pending()
        #print('%'*70)

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
        self.iter_deletes.append(order)
        self.add_pending(order, 'remove')


    # During orders iteration inserts don't occur, only qty updates and deletes.
    # Deletes are applied after iteration
    def apply_deletes(self):
        while self.iter_deletes:
            o = self.iter_deletes.pop()
            seq_key = self.seq_key(o)
            #seq_key = encode(o.price) + encode(self.valueId(o))

            del self.order_idx[o.id]
            self.deleted_order_idx[o.id] = o
            self.orders.remove(seq_key)

