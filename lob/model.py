# LOB Model

def encode(i): return int(i).to_bytes(8, 'big')
def decode(v): return int.from_bytes(v, 'big')

# Pack even sized field
def pack(v): return b''.join(v)
def unpack(f, s=8): return [f[i*s:(i*s)+s] for i in range(int(len(f) / s))]
# id,qty,price,account_id

# bids:
# price, pack(id, qty, account_id)

fsizes = (8,4,4,4)

class Base(object):
    def __init__(self):
        pass

class Quote(object):
    __slots__ = ['id', 'type', 'side', 'price', 'qty', 'account_id']

    def __init__(self, data):
        for k in __class__.__slots__:
            setattr(self, k, data[k])

    def __str__(self):
        pairs = [k + '=' + str(getattr(self, k)) for k in __class__.__slots__]
        return 'Quote(%s)' % ', '.join(pairs)


class Order(object):
    __slots__ = ['id', 'price', 'qty'] # 'account_id']

    #default = [
    #    'id:int', 'price:int', 'qty:int'
    #]
    def __init__(self, data):
        for k in __class__.__slots__:
            if type(data) == dict:
                setattr(self, k, data[k])
            elif type(data) == Quote:
                setattr(self, k, getattr(data, k))

    def list_kv(self):
        pass
        # return key, value

    def idx_kv(self):
        pass
        # return key(self.id), value(qty + account_id)

    def __str__(self):
        #return "%-10d @ %10.2f  tid:%-6d" % (self.qty, self.price, self.tid)
        pairs = [k + '=' + str(getattr(self, k)) for k in __class__.__slots__]
        return 'Order(%s)' % ', '.join(pairs)


class Account(object):
    __slots__ = ['id', 'balance', 'vol30d']

    def __init__(self):
        self.id = 1
        self.balance = 0
        self.vol30d = 0

