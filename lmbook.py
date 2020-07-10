import os
from pathlib import Path
from datetime import datetime, timedelta
import json
import shortuuid
import time
import sys
from collections import namedtuple
from decimal import Decimal
import lmdb

from sqlalchemy import create_engine, and_, or_, func
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import inspect

from model import (Ledger, Order)
from config import SQL, DT_FORMAT, DB_CONN

from operator import itemgetter, attrgetter

import dbm

engine = create_engine(DB_CONN)
session = Session(engine)

env = lmdb.open('bookdb', map_size=1024*1024*500, max_dbs=2)
buy_db = env.open_db(b'buy')
sell_db = env.open_db(b'sell')
dbs = {
    'buy': buy_db,
    'sell': sell_db
}


db = dbm.open('cachedb', 'c')


keys = ('id','price','market_id','account_id','side','status','balance')

def load_book():
    print('loading book..')


    book = {}
    orders = {
        'buy': (Order.price.desc(), Order.id.asc()),
        'sell': (Order.price.asc(), Order.id.asc())
    }
    for side in ('buy','sell'):
        start = time.time()
        q = session.query(
            Order.id,
            Order.price,
            Order.market_id,
            Order.account_id,
            Order.side,
            Order.status,
            Order.balance
        ).filter(
            Order.status.in_(('open','partial')),
            Order.side == side
        ).order_by(*orders[side])

        book[side] = []
        #with env.begin(write=True, db=dbs[side]) as txn:
        if True:
            for r in q.all():
                data = dict(zip(keys, r))
                for k in ('price','balance'):
                    data[k] = float(data[k])
                book[side].append(data)
                #key = str(int(data['price']*100000)) + ':' + str(data['id'])
                #value = ':'.join(map(lambda x: str(x), list(data.values())))
                #txn.put(key.encode(), value.encode())
        print("%d %s orders loaded in %.3f seconds." % (
            len(book[side]), side, time.time() - start))

    return book

def commit_book(book):
    pass


def query_book(book, side=None, price=None, account_id=None):
    if side == 'buy':
        out = list(filter(
            lambda x: x['account_id'] != account_id and x['price'] <= price,
            book['sell']
        ))
    else:
        out = list(filter(
            lambda x: x['account_id'] != account_id and x['price'] >= price,
            book['buy']
        ))

    """
    out = list(filter(
        lambda x:
            x['account_id'] != account_id and (
                side == 'buy' and x['price'] <= price or
                side == 'sell' and x['price'] >= price
            )
        ,book
    ))
    """

    # These sorts shouldn't be needed.
    #yes = True if side == 'sell' else False
    #out = sorted(out, key=itemgetter('id'))
    #out = sorted(out, key=itemgetter('price'), reverse=yes)

    return out


if __name__ == '__main__':

    book = load_book()


    for side in ('buy','sell'):
        start = time.time()
        cnt = 0
        with env.begin(db=dbs[side]) as txn:
            cursor = txn.cursor()
            for key, value in cursor.iternext(keys=True, values=True):
                cnt += 1
                data = dict(zip(keys, list(value.decode().split(':'))))
                #print(key.decode(), data)
        print("%s %d lmdb. %.4f seconds." % (
            side, cnt, time.time() - start))

    orders = [
        {'side':'buy','price':12.57,'account_id':1000},
        {'side':'sell','price':11.22,'account_id':1601}
    ]

    for o in orders:
        start = time.time()
        filtered = []
        print('\nORDER:',o)
        for r in query_book(book, **o):
            filtered.append(r)

        for i, v in enumerate(filtered[:3]):
            print('first '+str(i), v)
        print('..')
        for i, v in enumerate(filtered[-3:], start=-3):
            print('last '+str(i), v)

        other_side = 'sell' if o['side'] == 'buy' else 'buy'
        print("%d of %d %s orders filtered. Total %.4f seconds." % (
            len(filtered), len(book[other_side]), other_side,
            time.time() - start))

    print('-' * 75)
    print('get from lmdb now..')
    for o in orders:
        start = time.time()
        other_side = 'sell' if o['side'] == 'buy' else 'buy'

        filtered = []
        print('\nORDER:',o)

        """
        1. move cursor to first order in price stack
        2. iter thru until balance cleared:
            a. create trades/ledgers
            b. update order balances & status
        3. apply db updates:
            - order updates + del from lmdb
            - create new order + add to lmdb
            - delete event from queue(or status)
        """

        with env.begin(db=dbs[other_side]) as txn:
            cursor = txn.cursor()
            it = cursor.iterprev if other_side == 'buy' else cursor.iternext
            for key, value in it(keys=True, values=True):
                cnt += 1
                data = dict(zip(keys, list(value.decode().split(':'))))
                for k in ('price','balance'):
                    data[k] = float(data[k])

                if data['account_id'] == o['account_id']:
                    continue
                if o['side'] == 'buy' and data['price'] > o['price']:
                    continue
                elif o['side'] == 'sell' and data['price'] < o['price']:
                    continue
                filtered.append(data)
                #print(key.decode(), data)

        for i, v in enumerate(filtered[:3]):
            print('first '+str(i), v)
        print('..')
        for i, v in enumerate(filtered[-3:], start=-3):
            print('last '+str(i), v)

        print("%d of %d %s orders filtered. Total %.4f seconds." % (
            len(filtered), len(book[other_side]), other_side,
            time.time() - start))


