import os
from pathlib import Path
from datetime import datetime, timedelta
import json
import shortuuid
import time
from collections import namedtuple
from decimal import Decimal
import math
import copy

import redis
from rq import Connection, Queue, Worker
from rq.job import Job

from sqlalchemy import create_engine, and_, or_, func
from sqlalchemy.orm import Session, joinedload

import model
import config as cfg
from config import SQL, DT_FORMAT, DB_CONN
from model import (
    Account, Market, Asset, Event, Order, Trade, TradeSide, Ledger,
    FeeSchedule
)
from lib import random_dates, TradeFile
from ohlc import OHLC

from sqlalchemy.schema import CreateTable
from sqlalchemy.orm.session import make_transient

from dirqueue import Stats

BATCH_SIZE = 1

FEE_ACCOUNT_ID = 1

conn = redis.from_url(cfg.RQ_CONN)

XTS = Stats(('trade','add','cancel','all'))

class CustomJob(Job):
    def _execute(self):
        # EventRunner.run_one() knows how to call rq funcname
        getattr(self, '_evref').run_one(*self.args)

class SimpleWorker(Worker):
    job_class = CustomJob

    def __init__(self, *args, **kwargs):
        # Take in reference to EventRunner
        self._evref = kwargs.get('_evref')
        del kwargs['_evref']
        super().__init__(*args, **kwargs)

    def run_maintenance_tasks(self):
        print('run_maintenance_tasks',time.time())

    def main_work_horse(self, *args, **kwargs):
        raise NotImplementedError("Test worker does not implement this method")

    def execute_job(self, job, queue):
        # Give CustomJob a reference to EventRunner
        setattr(job, '_evref', self._evref)
        """Execute job in same thread/process, do not fork()"""
        timeout = (job.timeout or DEFAULT_WORKER_TTL) + 60
        return self.perform_job(job, queue, heartbeat_ttl=timeout)


"""
In memory data:

1. Account Balances (multiple markets need this)
    - Balance - reserve

2. Account Reserves (multiple markets need this)
    - Amount remaining on order and pending withdraw

3. 30d Account Volume (multiple markets need this)
    - How much traded within a month determines fee tier
    - Changes at time moves. To keep storage light, only
      maintain list of volume per day. This means that if
      volumes greatly increase or decrease in a day, there
      will be up to a 1 day lag in changing tiers.

4. Order Book (this can be single process)
    - Can I get the order book into memory? It doesn't need to be
      the entire thing, just a large enough chunk of it to cover
      the volume of trades coming in.
    - Proposal: Have two ordered lists. One for buys and one for sells.
      Always pop from the top of the list, stop when price exceeds limit.
      When list in memory is empty, fetch new batch from db.
      * When new order is added to db, it also needs to be added to the
        in memory list, in order.. right before the next price
      a. when a new order matches several orders and has some left over,
         the place to insert will be where it left off.
      b. when a new order doesn't match, it will just be looping until
         it finds that point. This can be slow if it's deep.
      c. will need to limit the batch in memory to cut off at the end
         of a price change, so they can just be appended to the end.

Output:


"""


class OrderCache():
    def __init__(self, db, market):
        self.db = db
        self.market = market

        self.once = 0
        self.buys = []
        self.sells = []

        # Init cache
        for side in ('buy','sell'):
            self.refill(side)

        self.buys_idx = -1
        self.sells_idx = -1

    def other_side(self, side):
        return 'sell' if side == 'buy' else 'buy'

    def refill(self, side):
        #orders = self.buys if side == 'buy' else self.sells
        #if len(orders):
        #    raise Exception('Attempt to refill non-empty cache')
        orders = self.get_orders(side)
        if side == 'buy':
            self.buys.extend(orders)
        elif side == 'sell':
            self.sells.extend(orders)
        print('refill() added %d orders to %s cache.' % (len(orders), side))

        print("buys: %d" % (len(self.buys,)))
        print("sells: %d" % (len(self.sells,)))


    def get_orders(self, side):
        db = self.db

        print('get_orders('+side+')')

        where = [
            Order.market_id == self.market.id,       # This market
            Order.status.in_(['open','partial']), # Open
            Order.side == side
        ]

        order = []

        # Ordering by side
        if side == 'buy':
            order.extend((Order.price.desc(), Order.id.asc()))
        elif side == 'sell':
            order.extend((Order.price.asc(), Order.id.asc()))

        # Query order matches in fifo order
        q = db.query(
            Order
        ).filter(and_(*where)).order_by(*order).limit(100)
        orders = []
        for o in q.all():
            orders.append(o)
            print("%-10d: %12.2f  %12.2f (%d)" % (
                o.id,
                o.price,
                o.amount,
                o.account_id
            ))

        return orders

    def get(self, idx=0):
        pass


    # next(side, account_id, price)
    def next(self, o):
        print('next()')
        self.reset()
        self.o = o
        self.iter_adv = True
        self.iter_side = self.other_side(o.side)
        return self

    def skip(self):
        if self.iter_side == 'buy':
            self.buys_idx += 1
        elif self.iter_side == 'sell':
            self.sells_idx += 1
        self.iter_adv = True

    def done(self):
        if self.iter_side == 'buy':
            o = self.buys.pop(0)
            self.buys_idx -= 1
        elif self.iter_side == 'sell':
            o = self.sells.pop(0)
            self.sells_idx -= 1

        self.iter_adv = True

    def getout(self):
        self.iter_adv = True

    def in_limit(self):
        orders = self.buys if self.iter_side == 'buy' else self.sells
        idx = self.buys_idx if self.iter_side == 'buy' else self.sells_idx

        o = self.o
        om = orders[idx]
        if o.side == 'buy' and om.price <= o.price:
            return True
        elif o.side == 'sell' and om.price >= o.price:
            return True
        return False

    def reset(self):
        self.buys_idx = -1
        self.sells_idx = -1

        print("buys: %d" % (len(self.buys,)))
        print("sells: %d" % (len(self.sells,)))

    # Mark done (remove from cache)
    # Skip (leave in cache, but advance index)
    # Reset (reset indexes to zero.. new order)

    def __iter__(self):
        return self

    def __next__(self):
        #if not self.iter_adv:
        #    raise Exception('Failed to complete interator')
        
        orders = self.buys if self.iter_side == 'buy' else self.sells
        idx = self.buys_idx if self.iter_side == 'buy' else self.sells_idx

        # Refill
        if not len(orders) or idx + 1 >= len(orders):
            if self.once > 0:
                print('Exceeded run limit.')
                raise StopIteration
            # Hmm.. if we refill in the iteration and the code doesn't remove stuff
            # from book, we'll just keep refilling the same.
            self.once += 1
            print('refilling..')
            self.refill(self.iter_side)

        if not len(orders):
            print('No more orders.')
            raise StopIteration

        #orders = self.buys if self.iter_side == 'buy' else self.sells

        # !!! The interation SHOULDN'T pop().. just cycle through index
        #return [self.orders.pop(0)]
        idx += 1
        if self.iter_side == 'buy':
            self.buys_idx = idx
        elif self.iter_side == 'sell':
            self.sells_idx = idx
        if idx >= len(orders):
            print('idx done.')
            raise StopIteration

        self.iter_adv = False
        return orders[idx]

class DepositWithdrawRunner():
    def __init__(self, session):
        db = self.session = session

        self.funcs = {
            'deposit'      : self.deposit,
            'withdraw'     : self.withdraw,
        }

        self.assets = {}

        q = db.query(Market).options(
            joinedload(Market.asset, innerjoin=True),
            joinedload(Market.uoa, innerjoin=True)
        )

        for a in db.query(Asset).all():
            self.assets[a.id] = a

        self._get_fee_schedule()

        self.tf = TradeFile()

    def run(self):
        #self.run_from_rq()
        #self.run_from_db()
        self.run_test()


    def run_from_db(self):
        db = self.session

        begin = time.time()

        events = db.query(Event).filter_by(status='new').\
            order_by(Event.created.asc()).limit(10000).all()
            #events = q.all()
        if not len(events):
            print('No events.')
            return

        print('Running %d events..' % len(events))

        chunk = events[:BATCH_SIZE]

        for e in chunk:
            self.funcs[e.method](e)
        #db.commit()
        #self.msession.commit()

        #self.tf.commit()
        elapsed = (time.time() - begin) * 1000
        print("Handled %d of %d events in %d ms. %.2f e/s" % (
            len(chunk), len(events), elapsed, len(chunk) / elapsed
        ))

    def deposit(self, e):
        p = json.loads(e.body)

        l = model.Ledger(
            account_id=p['account_id'],
            asset_id=p['asset_id'],
            amount=p['amount']
        )
        self.session.add(l)
        e.status = 'done'

    def withdraw(self, e):
        p = json.loads(e.body)

        l = model.Ledger(
            account_id=p['account_id'],
            asset_id=p['asset_id'],
            amount=p['amount'] * -1
        )
        self.session.add(l)
        e.status = 'done'


##################


# OrderExecution (place, cancel, amend)
class EventRunner():
    def __init__(self, session, market_code):
        db = self.session = session

        self.funcs = {
            'cancel-order' : self.cancel_order,
            'place-order'  : self.place_order
        }

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

        print('market_code:',market_code)
        print('market:',self.market.__dict__)

        self.assets = {}
        for a in db.query(Asset).all():
            self.assets[a.id] = a

        self._get_fee_schedule()

        self.tf = TradeFile()

    def run(self):
        #self.run_from_rq()
        self.run_from_db()
        #self.run_test()

    def run_test(self):
        db = self.session
        # load book
        # Store the end of the cache list.
        # Can we avoid looping the cache list everytime?
        # If buy isn't >= the first sell, just add to book
        # If sell isn't <= the first buy, just add to book
        # Any limit buy/sell starts matching on the first one...
        # ...and stops on the > or < the limit.
        # ..skipping account_id == mine
        # When at end of cache, refill it and keep going.


        orders = OrderCache(db, self.market)

        print('='*78)

        events = db.query(Event).filter(
            Event.status == 'new',
            Event.method.in_(['place-order'])
        ).order_by(Event.created.asc()).limit(1000).all()
        if not len(events):
            print('No events.')
            return

        e = events[0]
        # Create new order (add to session at the end)
        new_order = json.loads(e.body)
        for k in ('uuid','created','account_id'):
            new_order[k] = getattr(e, k)
        for k in ('price','amount'):
            new_order[k] = Decimal(new_order[k])
        new_order['balance'] = new_order['amount']
        o = Order(**new_order)

        print('[NEW ORDER] from account %d: buy %d @ %.2f' % (
            o.account_id,
            o.balance,
            o.price
        ))

        # The failure mode we want is for the queue to get stuck.
        # If an unknown failure occurs and we just skip it, then
        # market will get skewed.
        # Stuck queue will result in it getting backed up and orders
        # getting load shedded.
        # All failures that result in queue continuing must be 
        # explicit.
        # This means we DO NOT pop(0) until a complete is signaled.
        # Match loop calcuates all balance changes and outputs:
        #  - new order
        #  - trade
        #  - 6 ledgers
        #  - updates
        # Then all those run, on successful completion, item
        # is removed from queue.  If successful completion and item
        # fails to remove from queue, it will create duplicate runs.
        # Order creation handles duplicate via uuid. That condition should
        # halt queue.

        # Performance: If this is too slow, making all these writes per each
        # new order, maybe we can batch them.
        demand = o.balance
        for i, om in enumerate(orders.next(o)):
            if not demand:
                #orders.getout()
                break

            # If price within LIMIT AND account != mine:
            #   update balance
            #   create trade
            #   create ledgers
            # if balance == 0:
            #   update db and remove from cache
            note = ''
            tx_amt = 0
            if o.account_id == om.account_id:
                orders.skip()
                continue
            if not orders.in_limit():
                break

            note = 'done'
            tx_amt = om.balance if demand > om.balance else demand
            demand -= tx_amt

            print("%-4d consume %-10d: %12.2f  %d -> %d [%d] (%d) %s" % (
                i,om.id, om.price,
                om.balance, om.balance - tx_amt,
                demand,
                om.account_id, note
            ))

            if note == 'skip':
                orders.skip()
            elif note == 'done':
                orders.done()
                #om.balance = om.balance - tx_amt


    def run_from_rq(self):
        with Connection():
            queue = Queue('shtusd')
            worker = SimpleWorker([queue], connection=conn, _evref=self)
            worker.work(burst=False)
        print('done run()')

    def run_from_db(self):
        db = self.session

        begin = time.time()

        events = db.query(Event).filter(
            Event.status == 'new',
            Event.method.in_(['place-order','cancel-order'])
        ).order_by(Event.created.asc()).limit(1000).all()
        if not len(events):
            print('No events.')
            return

        print('Running %d events..' % len(events))

        self.orders = OrderCache(db, self.market)

        for e in events:
            s1 = time.time()
            result = self.funcs[e.method](e)
            db.commit()
            s2 = time.time()
            if e.method == 'place-order':
                if result:
                    XTS.set('trade', s2 - s1)
                else:
                    XTS.set('add', s2 - s1)
        #self.msession.commit()

        #self.tf.commit()
        elapsed = (time.time() - begin) * 1000
        print("Handled %d events in %d ms. %.2f e/s" % (
            len(events), elapsed, len(events) / elapsed
        ))
        XTS.set('all', time.time() - begin, len(events))

        XTS.print_stats()


    def run_one(self, data):
        print('EventRunner.run_one()')
        db = self.session
        begin = time.time()

        e = db.query(Event).filter_by(uuid=data['uuid']).one_or_none()
        if not e:
            print('ERROR No db event.')
            return

        self.funcs[e.method](e)

        #db.commit()
        #self.tf.commit()

        elapsed = time.time() - begin

        e.runtime = int(elapsed * 1000)
        #db.commit()

        #OHLC(self.session).update_cache(['shtusd'])


        print("run_once() handled in %.5f seconds." % (elapsed,))


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


    def place_order(self, e):

        # Create new order (add to session at the end)
        new_order = json.loads(e.body)
        for k in ('uuid','created','account_id'):
            new_order[k] = getattr(e, k)
        for k in ('price','amount'):
            new_order[k] = Decimal(new_order[k])
        new_order['balance'] = new_order['amount']
        o = Order(**new_order)

        market = self.market

        print("%-12s %6s %-4s %10.4f @ %10.4f   [account_id:%d]\n%s %23.23s %s" % (
            e.method, o.type, o.side, o.amount,
            o.price, e.account_id, e.created, '', e.uuid))

        # SET: market_id, account_id, price, side, status, balance, id
        # ORDER: price, id
        # FILTER: market_id, account_id, price, side, status

        ###########################################################
        """
        where = [
            Order.market_id == o.market_id,       # This market
            Order.account_id != e.account_id,     # Not mine
            Order.status.in_(['open','partial']), # Open
        ]
        order = []

        # Ordering by side
        if o.side == 'sell':
            where.extend((Order.side == 'buy', Order.price >= o.price))
            order.extend((Order.price.desc(), Order.id.asc()))
        elif o.side == 'buy':
            where.extend((Order.side == 'sell', Order.price <= o.price))
            order.extend((Order.price.asc(), Order.id.asc()))

        # Query order matches in fifo order
        q = self.session.query(Order).filter(and_(*where)).order_by(*order)

        bitches = q.all()
        """
        ###########################################################

        demand = o.amount

        make_trades = False

        # need to loop refill until empty or until break
        #for om in orders:
        for om in self.orders.next(o):

            
            # Fill until demand is empty
            if not demand:
                break

            if o.account_id == om.account_id:
                self.orders.skip()
                continue
            if not self.orders.in_limit():
                break




            # Get 30d account volume (TODO: Faster way to get this)
            """
            aq = self.session.query(
                Ledger.account_id,
                Ledger.asset_id,
                func.sum(Ledger.amount).label('volume'),
            ).filter(
                Ledger.account_id.in_((o.account_id, om.account_id)),
                Ledger.asset_id == market.uoa.id,
                Ledger.created >= datetime.utcnow() - timedelta(30)
            ).group_by(Ledger.account_id, Ledger.asset_id)
            """
            vol30d = {
                o.account_id: 0,
                om.account_id: 0
            }
            #for r in aq.all():
            #    vol30d[r.account_id] = r.volume


            # Query balance to update running balance in ledger

            accounts = (o.account_id, om.account_id, FEE_ACCOUNT_ID)
            """
            bq = self.session.query(
                Ledger.account_id,
                Ledger.asset_id,
                func.sum(Ledger.amount).label('balance'),
            ).filter(
                Ledger.account_id.in_(accounts),
                Ledger.asset_id.in_((market.asset.id, market.uoa.id))
            ).group_by(Ledger.account_id, Ledger.asset_id)
            """
            bal = {}

            for i in (market.asset.id, market.uoa.id):
                if i not in bal:
                    bal[i] = {}
                for j in accounts:
                    bal[i][j] = 0

            """
            for b in bq.all():
                if b.asset_id not in bal:
                    bal[b.asset_id] = {}
                bal[b.asset_id][b.account_id] = b.balance
            """
            tx_amt = om.balance if demand > om.balance else demand
            tx_total = tx_amt * om.price
            demand -= tx_amt

            t = Trade(
                uuid        = shortuuid.uuid(),
                created     = e.created,
                market_id   = market.id,
                price       = om.price,
                amount      = tx_amt,
            )

            # The rate is determined by the 30d volume (maker, taker)
            maker_rate = self._get_fee_rate('trade', vol30d[o.account_id])[0]
            taker_rate = self._get_fee_rate('trade', vol30d[om.account_id])[1]

            # fee comes out of both sides
            ts = TradeSide(
                uuid       = shortuuid.uuid(),
                account_id = o.account_id,
                trade      = t,
                order_uuid = o.uuid,
                type       = 'taker',
                fee_rate   = Decimal(taker_rate),
                amount     = t.amount if o.side == 'buy' else t.total,
            )
            ms = TradeSide(
                uuid       = shortuuid.uuid(),
                account_id = om.account_id,
                trade      = t,
                order_uuid = om.uuid,
                type       = 'maker',
                fee_rate   = Decimal(maker_rate),
                amount     = t.amount if om.side == 'buy' else t.total,
            )

            for x in (t, ts, ms):
                self.session.add(x)

            buyer_id  = e.account_id if o.side == 'buy' else om.account_id
            seller_id = e.account_id if o.side == 'sell' else om.account_id

            if e.account_id == buyer_id:
                bside = ts
                sside = ms
                amt_fee = (t.amount * ts.fee_rate)
                total_fee = (t.total * ms.fee_rate)
            else:
                bside = ms
                sside = ts
                amt_fee = (t.amount * ms.fee_rate)
                total_fee = (t.total * ts.fee_rate)

            keys = ['trade_side', 'account_id','asset_id','amount']
            ledgers = [
                (sside, seller_id,      market.asset.id,  tx_amt   * -1),
                (bside, buyer_id,       market.asset.id,  tx_amt - amt_fee),
                (None,  FEE_ACCOUNT_ID, market.asset.id,  amt_fee),

                (bside, buyer_id,       market.uoa.id,    tx_total * -1),
                (sside, seller_id,      market.uoa.id,    tx_total - total_fee),
                (None,  FEE_ACCOUNT_ID, market.uoa.id,    total_fee),
            ]

            # Create ledger entries
            print('-'*75)
            for values in ledgers:
                l = Ledger(**dict(zip(keys, values)))
                l.type = 'trade'
                if l.asset_id in bal and l.account_id in bal[l.asset_id]:
                    l.balance = bal[l.asset_id][l.account_id] + l.amount
                self.session.add(l)

                print("%3s %8d %15.2f" % (
                    self.assets[l.asset_id].symbol, l.account_id, l.amount))

            # Tee trade stream (todo)
            self.tf.append(market, ','.join([
                t.created.strftime(DT_FORMAT),
                "%.10f" % t.price,
                "%.10f" % t.amount
            ]))

            #om2 = copy.deepcopy(om)

            # New order balance and status (in memory)
            om.balance = om.balance - tx_amt
            #om[3] = om.balance - tx_amt
            om.status = self.get_order_status(om)
            #om[4] = self.get_order_status(om)

            make_trades = True
            print("%12s %10.4f  %10.4f @ %10.4f   [account_id:%d]\n   %s %23.23s %s" % (
            'fill', tx_amt, om.balance,
            om.price, om.account_id, om.created, '', e.uuid))

        o.balance = demand
        o.status = self.get_order_status(o)
        self.session.add(o)

        # Set event done
        e.status = 'done'

        print("%-12s %6s %-4s %10.4f @ %10.4f   [account_id:%d]\n%s %23.23s %s" % (
            e.method, o.type, o.side, o.balance,
            o.price, e.account_id, e.created, '', e.uuid))

        return make_trades

    def get_order_status(self, obj):
        if obj.balance == 0:
            return 'closed'
        elif obj.balance != obj.amount:
            return 'partial'
        else:
            return 'open'

    def cancel_order(self, e):
        p = json.loads(e.body)
        uuid = p['uuid']
        o = self.session.query(Order).filter(
            Order.uuid==uuid
        ).one_or_none()
        print(o.__dict__)
        if o.account_id != e.account_id:
            o.status = 'error'
        else:
            o.status = 'canceled'
        e.status = 'done'



"""
Fees

Trade 5 @ $20.00 (Eric buys)

Method #1 (balanced; fees):            unit of account (balanced)
SHT Wayne   -5.00                      -100.00
USD Eric  -100.00                      -100.00
SHT Eric     4.90 (2% taker: 0.10)       98.00
USD Wayne   99.00 (1% maker: 1.00)       99.00
USD Exchg    1.00                         1.00
SHT Exchg    0.10                         2.00

Method #2 (unbalanced; convert fee):   unit of account (balanced)
SHT Wayne   -5.00                      -100.00
USD Eric  -100.00                      -100.00
SHT Eric     4.90 (2% taker: 0.10)       98.00
USD Wayne   99.00 (1% maker: 1.00)       99.00
USD Exchg    3.00 (convert to USD)        3.00

"""


