# LOB Model

def encode(i): return int(i).to_bytes(8, 'big', signed=True)
def decode(v): return int.from_bytes(v, 'big', signed=True)

# Pack even sized field
def pack(v): return b''.join(v)
def unpack(f, s=8): return [f[i*s:(i*s)+s] for i in range(int(len(f) / s))]
# id,qty,price,account_id


class Column(object):
    __slots__ = ['name', 'type', 'required', 'default']

    def __init__(self, name, t, required=False, default=None):
        self.name = name
        self.type = t
        self.required = required
        self.default = default

    def __str__(self):
        return self.name

class Base(object):
    __slots__ = []

    def __init__(self, data=None, **kwargs):
        data = kwargs if kwargs else data
        if not data:
            raise Exception(type(self).__name__ + ': Missing args.')
        for c in self.cols:
            value = data.get(c.name, None)
            if value == None and c.default != None:
                value = c.default
            if value == None and c.required == True:
                raise Exception(c.name + ' required')
            elif value != None and type(value) != c.type:
                raise TypeError(type(self), c.name, c.type)

            setattr(self, c.name, value)

        post_validate = getattr(self, 'post_validate', None)
        if post_validate:
            post_validate()

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.cols}

    def __str__(self):
        name = self.__class__.__name__
        pairs = [c.name + '=' + str(getattr(self, c.name)) for c in self.cols]
        return '%s(%s)' % (name, ', '.join(pairs))


enum_type = set(('limit','market'))
enum_side = set(('bid','ask'))
class Quote(Base):
    cols = (
        Column('id',         int, required=True),
        Column('type',       str, required=True),
        Column('side',       str, required=True),
        Column('price',      int, required=False), # Only req for limit
        Column('qty',        int, required=True),
        Column('account_id', int, required=False),
    )

    __slots__ = [c.name for c in cols]

    def post_validate(self):
        if self.type == 'limit' and not self.price:
            raise Exception('Price missing for limit order')


class Order(Base):
    cols = (
        Column('id',         int, required=True),
        Column('price',      int, required=True),
        Column('qty',        int, required=True),
        Column('account_id', int, required=False),
    )

    __slots__ = [c.name for c in cols]


class Trade(Base):
    cols = (
        Column('time',  int, required=True),
        Column('price', int, required=True),
        Column('qty',   int, required=True)
    )

    __slots__ = [c.name for c in cols]


class Account(Base):
    cols = (
        Column('id',      int, required=True),
        Column('balance', int, required=True),
        Column('vol30d',  int, required=True)
    )

    __slots__ = [c.name for c in cols]

