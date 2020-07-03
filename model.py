from datetime import datetime
import shortuuid

from sqlalchemy import (
    create_engine,
    Table, Column,
    Integer, BigInteger, Boolean, String, Text,
    Numeric, Enum, DateTime, Date, Float, JSON,
    ForeignKey, UniqueConstraint, ForeignKeyConstraint,
    PrimaryKeyConstraint
)
#from sqlalchemy.types import JSON
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
MoneyColumn = Column(Numeric(20,10), default=0)


def tables():
    entity = {}
    for c in Base._decl_class_registry.values():
        if hasattr(c, '__table__'):
            name = c.__table__.name
            entity[name] = c
    return entity

# Exchange data

"""
type = Column(Enum('trade', name='fee_type'), default='trade', nullable=False)
method = Column(Enum('place-order','cancel-order','deposit', 'withdraw', name='event_method'), nullable=False)
status = Column(Enum('new','done', name='event_status'), default='new')
side = Column(Enum('buy','sell', name='order_side'), nullable=False)
type = Column(Enum('limit','market', name='order_type'), nullable=False)
status = Column(Enum('open','partial','closed','canceled', name='order_status'), default='open')
side = Column(Enum('buy','sell', name='trade_side'), nullable=False)
type = Column(Enum('maker','taker', name='trade_side_type'), nullable=False)
type = Column(Enum('deposit','withdraw','trade', name='ledger_type'), nullable=False)
"""

class Asset(Base):
    __tablename__ = 'asset'

    id = Column(Integer, primary_key=True)
    uuid = Column(String(22), default=shortuuid.uuid,
        nullable=False, unique=True, index=True)

    symbol = Column(String(10))
    icon = Column(String(50))
    name = Column(String(255))
    scale = Column(Integer) # digits behind decimal

    created = Column(DateTime, default=utcnow)
    modified = Column(DateTime, onupdate=utcnow)

class Market(Base):
    __tablename__ = 'market'

    id = Column(Integer, primary_key=True)
    uuid = Column(String(22), default=shortuuid.uuid,
        nullable=False, unique=True, index=True)

    code = Column(String(15), nullable=True)
    name = Column(String(255), nullable=True)

    asset1 = Column(Integer, ForeignKey('asset.id'), nullable=False)
    asset = relationship("Asset", foreign_keys=[asset1])
    asset2 = Column(Integer, ForeignKey('asset.id'), nullable=False)
    uoa = relationship("Asset", foreign_keys=[asset2])

    created = Column(DateTime, default=utcnow)
    modified = Column(DateTime, onupdate=utcnow)

class Account(Base):
    __tablename__ = 'account'

    id = Column(Integer, primary_key=True)
    uuid = Column(String(22), default=shortuuid.uuid,
        nullable=False, unique=True, index=True)

    email = Column(String(255), nullable=False, unique=True)
    name = Column(String(255), nullable=False)
    location = Column(String(255), nullable=False)
    title = Column(String(255), nullable=False)

    created = Column(DateTime, default=utcnow)
    modified = Column(DateTime, onupdate=utcnow)

class AccountAsset(Base):
    __tablename__ = 'account_asset'

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, index=True)
    asset_id = Column(Integer, index=True)
    balance = MoneyColumn.copy()
    vol30d = Column(Integer, default=0)

class FeeSchedule(Base):
    __tablename__ = 'fee_schedule'

    id = Column(Integer, primary_key=True)
    type = Column(Enum('trade', name='fee_type'), default='trade', nullable=False)
    volume = Column(Integer, nullable=False)
    maker = Column(Integer, nullable=False)
    taker = Column(Integer, nullable=False)


class Event(Base):
    __tablename__ = 'event'

    id = Column(Integer, primary_key=True)
    uuid = Column(String(22), default=shortuuid.uuid,
        nullable=False, unique=True, index=True)

    method = Column(Enum(
        'place-order','cancel-order',
        'deposit', 'withdraw',
        name='event_method'
    ), nullable=False)
    body = Column(Text()) # json payload
    status = Column(Enum('new','done', name='event_status'), default='new')

    account_id = Column(Integer, ForeignKey('account.id'), nullable=True)
    account = relationship("Account")

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
    uuid = Column(String(22), default=shortuuid.uuid,
        nullable=False, unique=True, index=True)

    account_id = Column(Integer, ForeignKey('account.id'), nullable=False)
    account = relationship("Account")
    market_id = Column(Integer, ForeignKey('market.id'), nullable=False)
    market = relationship("Market")
    
    price = MoneyColumn.copy()
    amount = MoneyColumn.copy()
    balance = MoneyColumn.copy()

    side = Column(Enum('buy','sell', name='order_side'), nullable=False)
    type = Column(Enum('limit','market', name='order_type'), nullable=False)
    status = Column(Enum('open','partial','closed','canceled', name='order_status'), default='open')
    # open, partial, closed, canceled
    # open, partial orders should be deducted from account balance. It is reserved
    created = Column(DateTime, default=utcnow)
    modified = Column(DateTime, onupdate=utcnow)

"""
class MemOrder(Base):  # Order book state
    __tablename__ = 'mem_order'
    id = Column(Integer, primary_key=True)
    market_id = Column(Integer, index=True)
    account_id = Column(Integer, index=True)
    price = MoneyColumn.copy()
    balance = MoneyColumn.copy()
    side = Column(Enum('buy','sell'), nullable=False)
"""


# Deposit/Withdraw Request or PendingLedger
# Amounts do not become available to trade/withdraw until x
# number of confirmations.
# The other party cares about withdraw confirmations. The exchange
# cares about wd confirmations for balanced books.  If a wd doesn't
# confirm... need to handle
#class OutsideRequest(Base):
#    __tablename__ = 'outside_request'

class Trade(Base): # Append only
    __tablename__ = 'trade'

    id = Column(Integer, primary_key=True)
    uuid = Column(String(22), default=shortuuid.uuid,
        nullable=False, unique=True, index=True)

    market_id = Column(Integer, ForeignKey('market.id'), nullable=False)
    market = relationship("Market")

    price = MoneyColumn.copy()
    amount = MoneyColumn.copy()

    @hybrid_property
    def total(self):
        return self.price * self.amount

    created = Column(DateTime, default=utcnow, index=True)

class TradeSide(Base): # Append only
    __tablename__ = 'trade_side'

    id = Column(Integer, primary_key=True)
    uuid = Column(String(22), default=shortuuid.uuid,
        nullable=False, unique=True, index=True)

    type = Column(Enum('maker','taker', name='trade_type'), nullable=False)

    trade_uuid = Column(String(22), ForeignKey('trade.uuid'), nullable=False)
    trade = relationship("Trade")
    account_id = Column(Integer, ForeignKey('account.id'), nullable=False)
    account = relationship("Account")
    order_uuid = Column(String(22), ForeignKey('order.uuid'), nullable=True)
    order = relationship("Order")

    fee_rate = MoneyColumn.copy()
    amount = MoneyColumn.copy()

    @hybrid_property
    def fee(self):
        return self.amount * self.fee_rate

    created = Column(DateTime, default=utcnow, index=True)

class Ledger(Base): # Append only
    __tablename__ = 'ledger'

    id = Column(Integer, primary_key=True)
    uuid = Column(String(22), default=shortuuid.uuid,
        nullable=False, unique=True, index=True)
    type = Column(Enum('deposit','withdraw','trade', name='ledger_type'), nullable=False)

    account_id = Column(Integer, ForeignKey('account.id'), nullable=False,
        index=True)
    account = relationship("Account")

    asset_id = Column(Integer, ForeignKey('asset.id'), nullable=True)
    asset = relationship("Asset")

    amount = MoneyColumn.copy()
    balance = MoneyColumn.copy()

    # Origination References
    # Are all ledger entries tied to Order and Trade?
    trade_side_id = Column(Integer, ForeignKey('trade_side.id'), nullable=True)
    trade_side = relationship("TradeSide")

    created = Column(DateTime, default=utcnow)

