from collections import namedtuple
import json
import os
import re
import random
from sqlalchemy import and_, or_, func
from sqlalchemy.orm import joinedload
import time

from lob.orderbook import OrderBook, Quote

import config as cfg
from model import Market, Asset, FeeSchedule, Event

BATCH_SIZE = 1000

import lmdb

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

        db_path = str(cfg.CACHE_DIR / market_code / cfg.LOB_LMDB_NAME)
        self.env = lmdb.open(db_path, max_dbs=3, map_size=cfg.LOB_LMDB_SIZE)

        self.lob = OrderBook(self.env)

        self.assets = {}
        for a in db.query(Asset).all():
            self.assets[a.id] = a

        self._get_fee_schedule()


    def run(self):
        #self.run_from_rq()
        #self.run_from_db()
        self.run_from_file()

    def run_from_rq(self):
        with Connection():
            queue = Queue(self.market.code)
            worker = SimpleWorker([queue], connection=conn, _evref=self)
            worker.work(burst=False)
        print('done run()')

    def seqTime(self):
        return int(time.time() * 1000 * 1000 * 10)

    def samples(self):
        start = 500000000
        #samples = list(reversed(list(range(start,start+400))))
        samples = list(set([random.randint(1,start) for i in range(600)]))
        ids = random.sample(samples, 300)

        orders = []
        id = 0
        account = 237331
        MID_RANGE = 100

        for i in list(range(1,11)) + list((3,3,4,5,5,5)):
            id += 1
            qty = random.randint(5,10)
            q = Quote(id=self.seqTime()+id, type='limit', side='bid', qty=qty,
                price=100-i, account_id=id + account)
            orders.append(q)

        for i in list(range(1,10)) + list((3,4,4,4,5)):
            id += 1
            qty = random.randint(5,10)
            q = Quote(id=self.seqTime()+id, type='limit', side='ask', qty=qty,
                price=100+i, account_id=id + account)
            orders.append(q)

        id += 1
        q = Quote(id=self.seqTime()+id, type='market', side='bid', qty=50,
            account_id=id + account)
        orders.append(q)

        id += 1
        q = Quote(id=self.seqTime()+id, type='market', side='ask', qty=50,
            account_id=id + account)
        orders.append(q)

        id += 1
        q = Quote(id=self.seqTime()+id, type='market', side='ask', qty=9,
            account_id=id + account)
        orders.append(q)

        return orders

    def run_from_file(self):
        self.total_orders = 0
        self.total_trades = 0
        self.total_writes = 0

        #with open('quotes') as f:
        if True:
            account_id = 10000
            cnt = 0
            #for row in f.readlines():
            for quote in self.samples():
                """
                account_id += 1
                cnt += 1
                if cnt < 499700:
                    continue
                if cnt > 500100:
                    break
                (id, side, qty, price) = row.rstrip('\n').split(',')
                if cnt == 500100:
                    qty = 200
                quote = Quote(id=id, type='limit', side=side,
                    qty=int(qty), price=int(price), account_id=account_id)
                """
                self.run_one(quote)
            #if cnt % 1000 == 0:
            #    self.lob.tapeDump(self.trade_file, 'a', 'wipe')
            #    self.total_writes += 1


        print(self.lob)

        self.lob.tapeDump(self.trade_file, 'a', 'wipe')
        self.total_writes += 1

        self.lob.commit()

        print('orders:%d trades:%d writes:%d' % (
            self.total_orders,self.total_trades,self.total_writes))

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
            quote = Quote({
                'id'         : e.id,
                'type'       : o.type,
                'side'       : o.side,
                'qty'        : int(o.amount),
                'price'      : int(o.price),
                'account_id' : e.account_id
            })

            self.run_one(quote)

        self.lob.tapeDump(self.trade_file, 'a', 'wipe')
        self.total_writes += 1

        print(self.lob)
        self.lob.commit()

        print('orders:%d trades:%d writes:%d' % (
            self.total_orders,self.total_trades,self.total_writes))

    def run_one(self, quote):
        start = time.time()

        trades, orderInBook = self.lob.processOrder(quote)
        #OHLC(self.session).update_cache(['shtusd'])

        self.total_orders += 1
        self.total_trades += len(trades)

        elapsed = time.time() - start
        #foo = "  %.5f ms" % (elapsed * 1000,)
        #print('--> trades:',len(trades),'orderInBook:',bool(orderInBook), foo)
        #print()

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


