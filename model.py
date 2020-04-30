from datetime import datetime

from sqlalchemy import (
    create_engine,
    Column, Integer, BigInteger, Boolean, String, Text,
    DateTime, Date, Float,
    ForeignKey, UniqueConstraint, ForeignKeyConstraint,
    PrimaryKeyConstraint,
    and_
)
from sqlalchemy.orm import relationship, Session, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property


Base = declarative_base()

def dict_factory(cursor, row):
  d = {}
  for idx, col in enumerate(cursor.description):
    d[col[0]] = row[idx]
  return d

class Account(Base):
    __tablename__ = 'account'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=True)

class Market(Base):
    __tablename__ = 'market'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=True)

class Tx(Base):
    __tablename__ = 'tx'

    id = Column(Integer, primary_key=True)
    src_account_id = Column(Integer)
    dst_account_id = Column(Integer)
    price = Column(Integer)
    amount = Column(Integer)

class OrderBook(Base):
    __tablename__ = 'order_book'

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer)
    price = Column(Integer)
    amount = Column(Integer)
    test = Column(Integer)

