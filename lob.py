from collections import namedtuple
import json
import os
import re
import random
from sqlalchemy import and_, or_, func
from sqlalchemy.orm import joinedload
import time

from PyLOB.orderbook import OrderBook

import config as cfg
from model import Market, Asset, FeeSchedule, Event

BATCH_SIZE = 1000


# OrderExecution (place, cancel, amend)
class OrderBookRunner():
    def __init__(self, session, market_code):
        db = self.session = session

        # Load this market
        self.market = db.query(
            Market
        ).filter_by(
            code=market_code
        ).options(
            joinedload(Market.asset, innerjoin=True),
            joinedload(Market.uoa, innerjoin=True)
        ).one_or_none()

        # if not self.market: FAIL!

        print('market:',market_code)

        self.trade_file = cfg.CACHE_DIR / market_code / 'trades.log'

        db_path = cfg.CACHE_DIR / market_code / cfg.LOB_LMDB_NAME
        print('db_path:',db_path)
        self.lob = OrderBook(db_path, cfg.LOB_LMDB_SIZE)

        self.assets = {}
        for a in db.query(Asset).all():
            self.assets[a.id] = a

        self._get_fee_schedule()


    def run(self):
        #self.run_from_rq()
        self.run_from_db()

    def run_from_rq(self):
        with Connection():
            queue = Queue(self.market.code)
            worker = SimpleWorker([queue], connection=conn, _evref=self)
            worker.work(burst=False)
        print('done run()')

    def run_from_db(self):
        db = self.session


        self.total_orders = 0
        self.total_trades = 0
        self.total_writes = 0

        events = db.query(Event).filter(
            Event.status == 'new',
            Event.method.in_(['place-order','cancel-order'])
        ).order_by(Event.created.asc()).limit(1000).all()
        if not len(events):
            print('No events.')
            return

        print('Running %d events..' % len(events))
        sides = {
            'buy': 'bid',
            'sell': 'ask'
        }

        for e in events:
            d = json.loads(e.body)
            d['side'] = sides[d['side']]

            o = namedtuple('eventBody', d.keys())(*d.values())

            print(e.id, o)
            quote = {
                'type': o.type,
                'side': o.side,
                'amount': o.amount,
                'price': o.price,
                'id': e.id    # need a seq number, use this for now
            }

            self.run_one(quote)

        self.lob.tapeDump(self.trade_file, 'a', 'wipe')
        self.total_writes += 1

        print(self.lob)
        self.lob.commit()

        print('orders:%d trades:%d writes:%d' % (
            self.total_orders,self.total_trades,self.total_writes))
        

    def run_one(self, quote):
        start = time.time()
        quote = {
            'type'  : quote['type'],
            'side'  : quote['side'],
            'qty'   : int(quote['amount']),
            'price' : int(quote['price']),
            'tid'   : quote['id'],
            'idNum' : quote['id']
        }
        #print('run_one():',order)

        trades, orderInBook = self.lob.processOrder(quote)
        #OHLC(self.session).update_cache(['shtusd'])

        self.total_orders += 1
        self.total_trades += len(trades)

        elapsed = time.time() - start
        foo = "  %.5f ms" % (elapsed * 1000,)
        print('--> trades:',len(trades),'orderInBook:',bool(orderInBook), foo)
        print()

    def _get_fee_schedule(self):
        self.sched = {}
        q = self.session.query(FeeSchedule).order_by(
            FeeSchedule.type.asc(),
            FeeSchedule.volume.desc()
        )
        for r in q.all():
            if r.type not in self.sched:
                self.sched[r.type] = []
            self.sched[r.type].append({
                'min': r.volume,
                'maker': r.maker / 10000,
                'taker': r.taker / 10000
            })

    def _get_fee_rate(self, t, value):
        rates = [None,None]
        for r in self.sched[t]:
            rates = [r['maker'], r['taker']]
            if r['min'] < value:
                break

        return rates

    def get_body(self, e):
        return json.loads(e.body, object_hook=lambda d: namedtuple('eventBody', d.keys())(*d.values()))


