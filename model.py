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

class Asset(Base):
    __tablename__ = 'asset'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=True)

class Market(Base):
    __tablename__ = 'market'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=True)
    asset1 = Column(Integer) # fk Asset
    asset2 = Column(Integer) # fk Asset

class Order(Base):
    __tablename__ = 'order'

    id = Column(Integer, primary_key=True)
    account = Column(Integer) # fk Account
    price = Column(Integer)
    amount = Column(Integer)

class Transaction(Base):
    __tablename__ = 'transaction'

    id = Column(Integer, primary_key=True)
    account1 = Column(Integer) # fk Account
    account2 = Column(Integer) # fk Account
    price = Column(Integer)
    amount = Column(Integer)


