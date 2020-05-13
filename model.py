from datetime import datetime

from sqlalchemy import (
    Table,
    create_engine,
    Column, Integer, BigInteger, Boolean, String, Text,
    Numeric,
    DateTime,
    Date, Float,
    ForeignKey, UniqueConstraint, ForeignKeyConstraint,
    PrimaryKeyConstraint,
    and_
)
from sqlalchemy.orm import relationship, Session, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property

import re
from sqlalchemy.dialects.sqlite import DATETIME

"""
DateTime = DATETIME(
    storage_format="%(year)04d-%(month)02d-%(day)02d " + \
        "%(hour)02d:%(minute)02d:%(second)02d",
    regexp=r"(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)"
)
"""

class Base(object):
    @classmethod
    def __table_cls__(cls, *args, **kwargs):
        t = Table(*args, **kwargs)
        t.decl_class = cls
        return t

Base = declarative_base(cls=Base)

def utcnow():
    return datetime.utcnow()
    #return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

# Exchange data

class Asset(Base):
    __tablename__ = 'asset'

    id = Column(Integer, primary_key=True)
    symbol = Column(String(10))
    icon = Column(String(50))
    name = Column(String(255))
    scale = Column(Integer) # digits behind decimal

class Market(Base):
    __tablename__ = 'market'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=True)
    asset1 = Column(Integer) # fk Asset
    asset2 = Column(Integer) # fk Asset

class Owner(Base):
    __tablename__ = 'owner'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=True)
    email = Column(String(255), nullable=True)
    title = Column(String(255), nullable=True)
    created = Column(DateTime, default=utcnow)

class Event(Base):
    __tablename__ = 'event'

    id = Column(Integer, primary_key=True)
    owner = Column(Integer)
    action = Column(String(10)) # ao(add order), co(cancel order)
                               # dep wthd
    payload = Column(Text())
    created = Column(DateTime, default=utcnow)
    status = Column(String(10), default='new') # new, done

class Order(Base): # tx data; Cancel only
    __tablename__ = 'order'

    id = Column(Integer, primary_key=True)
    owner = Column(Integer) # fk Owner
    #market = Column(Integer) # fk Market
    
    
    market_id = Column(Integer, ForeignKey('market.id'), nullable=False)
    market = relationship("Market")

    
    direction = Column(String(16)) # buy, sell
    type = Column(String(16), default='limit') # limit, market
    price = Column(Integer) # when market, no price
    amount = Column(Integer)
    amount_left = Column(Integer)
    balance = Column(Integer)
    status = Column(String(16), default='new')
    # new, open, partial, close, cancel
    # open, partial orders should be deducted from balance. It is reserved
    created = Column(DateTime, default=utcnow)

class Trade(Base):
    __tablename__ = 'trade'

    id = Column(Integer, primary_key=True)
    market = Column(Integer)
    created = Column(DateTime, default=utcnow)
    price = Column(Numeric(18,8), default=0)
    amount = Column(Numeric(18,8), default=0)

class Transaction(Base): # tx data; Append only
    __tablename__ = 'transaction'

    id = Column(Integer, primary_key=True)
    created = Column(DateTime, default=utcnow)
    """
    account1 = Column(Integer) # fk Account (from)
    asset1 = Column(Integer) # fk Asset (from)

    account2 = Column(Integer) # fk Account (to)
    asset2 = Column(Integer) # fk Asset (to)

    price = Column(Integer)
    amount = Column(Integer)
    """

class TransactionItem(Base): # tx data; Append only
    __tablename__ = 'transaction_item'

    id = Column(Integer, primary_key=True)
    transaction = Column(Integer) # fk Transaction
    account = Column(Integer) #fk Account
    amount = Column(Integer) # signed?
    #order = Column(Integer) # fk Order



