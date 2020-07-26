from lob.model import Order, encode, decode
from stats import Stats, get_size, sizefmt

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

        #self.txn = env.begin(db=self.db)
        #self.cur = self.txn.cursor()

        # Hydrate any collections. Need to know if we can iterate

        self.queue = self.getlist()

    def __iter__(self):
        print('get iter..')
        # prime pump
        #self.queue = self.getlist()
        self.itxn = self.env.begin(db=self.db)
        self.icur = self.itxn.cursor()

        if self.side == 'bid':
            self.icur.last()
        elif self.side == 'ask':
            self.icur.first()

        return self

    def __len__(self):
        print('get len..')
        # return len of primed pump here
        return 0
        #return len(self.queue)

    def __next__(self):
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


    # Actions
    # 1. insert order - if inmemory copy, needs to insert there too
    # 2. delete order - if inmemory copy, delete there too
    # 3. set qty - update memory copy
    # 4. reset on each new iteration
    # 5. skip due to account!=account
    # 6. skip happens when trade doesn't take all qty (update)

    # skip is just continue.. easy
    # reset already happens
    # dont pop(0)
    # update qty (in list and on disk)

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

    @TS.timeit
    def updateQty(self, order, qty):
        txn = self.env.begin(write=True)
        res = txn.put(encode(order.id), encode(qty), db=self.idb)
        print('updateQty('+str(qty)+') res:',res)
        txn.commit()

    @TS.timeit
    def insertOrder(self, quote):
        order = Order(quote.to_dict())
        #self.volume += order.qty

        txn = self.env.begin(write=True)
        res1 = txn.put(encode(order.id), encode(order.qty), db=self.idb)
        res2 = txn.put(encode(order.price), encode(self.valueId(order)), db=self.db)
        print('insertOrder res:',res1, res2)

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
        #self.nOrders -= 1
        txn = self.env.begin(write=True)
        res1 = txn.delete(encode(order.price), encode(self.valueId(order)), db=self.db)
        res2 = txn.delete(encode(order.id), db=self.idb)
        print('removeOrder res:',res1, res2)

        txn.commit()

