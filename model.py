from datetime import datetime

from sqlalchemy import (
    create_engine,
    Table, Column,
    Integer, BigInteger, Boolean, String, Text,
    Numeric, Enum, DateTime, Date, Float,
    ForeignKey, UniqueConstraint, ForeignKeyConstraint,
    PrimaryKeyConstraint
)
from sqlalchemy.orm import relationship, Session, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property


class Base(object):
    @classmethod
    def __table_cls__(cls, *args, **kwargs):
        t = Table(*args, **kwargs)
        t.decl_class = cls
        return t

Base = declarative_base(cls=Base)


# Common bits
def utcnow():
    return datetime.utcnow()

#MoneyColumn = Column(BigInteger, default=0)
#MoneyColumn = Column(Numeric(19,9), default=0)
MoneyColumn = Column(Numeric(20,10), default=0)

def get_model_by_name(name):
    for c in Base._decl_class_registry.values():
        if hasattr(c, '__table__') and c.__table__.name == name:
            return c


# Exchange data

class Asset(Base):
    __tablename__ = 'asset'

    id = Column(Integer, primary_key=True)
    symbol = Column(String(10))
    icon = Column(String(50))
    name = Column(String(255))
    scale = Column(Integer) # digits behind decimal

    created = Column(DateTime, default=utcnow)
    modified = Column(DateTime, onupdate=utcnow)

class Market(Base):
    __tablename__ = 'market'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=True)
    asset1 = Column(Integer) # fk Asset
    asset2 = Column(Integer) # fk Asset

    created = Column(DateTime, default=utcnow)
    modified = Column(DateTime, onupdate=utcnow)

class Owner(Base):
    __tablename__ = 'owner'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=True)
    email = Column(String(255), nullable=True)
    title = Column(String(255), nullable=True)

    created = Column(DateTime, default=utcnow)
    modified = Column(DateTime, onupdate=utcnow)

class Event(Base):
    __tablename__ = 'event'

    id = Column(Integer, primary_key=True)
    method = Column(Enum(
        'place-order','cancel-order',
        'deposit', 'withdraw'
    ), nullable=False)
    body = Column(Text()) # json payload
    status = Column(Enum('new','done'), default='new')

    created = Column(DateTime, default=utcnow)
    modified = Column(DateTime, onupdate=utcnow)

class Order(Base): # Append only, except balance & status
    __tablename__ = 'order'

    def __init__(self, **kwargs):
        # Balance always starts off as amount
        if 'balance' not in kwargs:
            kwargs['balance'] = kwargs['amount']
        super(Order, self).__init__(**kwargs)

    id = Column(Integer, primary_key=True)
    owner_id = Column(Integer, ForeignKey('owner.id'), nullable=False)
    owner = relationship("Owner")
    market_id = Column(Integer, ForeignKey('market.id'), nullable=False)
    market = relationship("Market")
    
    price = MoneyColumn.copy()
    amount = MoneyColumn.copy()
    balance = MoneyColumn.copy()

    side = Column(Enum('buy','sell'), nullable=False)
    type = Column(Enum('limit','market'), nullable=False)
    status = Column(Enum('open','partial','closed','canceled'), default='open')
    # open, partial, closed, canceled
    # open, partial orders should be deducted from account balance. It is reserved
    created = Column(DateTime, default=utcnow)
    modified = Column(DateTime, onupdate=utcnow)

class Trade(Base): # Append only
    __tablename__ = 'trade'

    id = Column(Integer, primary_key=True)
    owner_id = Column(Integer, ForeignKey('owner.id'), nullable=False)
    owner = relationship("Owner")
    market_id = Column(Integer, ForeignKey('market.id'), nullable=False)
    market = relationship("Market")

    price = MoneyColumn.copy()
    amount = MoneyColumn.copy()

    order_id = Column(Integer, ForeignKey('order.id'), nullable=True)
    order = relationship("Order")

    created = Column(DateTime, default=utcnow)

class Ledger(Base): # Append only
    __tablename__ = 'ledger'

    id = Column(Integer, primary_key=True)
    owner_id = Column(Integer, ForeignKey('owner.id'), nullable=False)
    owner = relationship("Owner")

    asset_id = Column(Integer, ForeignKey('asset.id'), nullable=True)
    asset = relationship("Asset")

    amount = MoneyColumn.copy()
    balance = MoneyColumn.copy()

    # Origination References
    # Are all ledger entries tied to Order and Trade?
    order_id = Column(Integer, ForeignKey('order.id'), nullable=True)
    order = relationship("Order")
    trade_id = Column(Integer, ForeignKey('trade.id'), nullable=True)
    trade = relationship("Trade")

    created = Column(DateTime, default=utcnow)

