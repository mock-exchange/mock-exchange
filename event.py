import os
from pathlib import Path
from datetime import datetime, timedelta
import json
import shortuuid
import time
from collections import namedtuple
from decimal import Decimal
import math

from sqlalchemy import create_engine, and_, or_, func
from sqlalchemy.orm import Session, joinedload

import model
from config import SQL, DT_FORMAT
from model import (
    Account, Market, Asset, Event, Order, Trade, TradeSide, Ledger,
    FeeSchedule
)
from lib import random_dates, TradeFile

BATCH_SIZE = 1000

FEE_ACCOUNT_ID = 1

class EventRunner():
    def __init__(self, session):
        self.session = session

        self.funcs = {
            'deposit'      : self.event_deposit,
            'withdraw'     : self.event_withdraw,
            'cancel-order' : self.event_cancel_order,
            'place-order'  : self.event_place_order
        }

    def _get_markets(self, markets=['all']):
        filters = []
        if 'all' not in markets:
            filters.append(Market.code.in_(markets))
        q = self.db.query(
            Market.id,
            Market.code,
            Market.name,
            func.min(Trade.created).label('first_trade'),
            func.max(Trade.created).label('last_trade')
        ).join(Trade).filter(*filters).group_by(Market.id)

        return q.all()

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

    def run(self):
        db = self.session

        q = db.query(Market).options(
            joinedload(Market.asset, innerjoin=True),
            joinedload(Market.uoa, innerjoin=True)
        )
        self.markets = {}

        for m in q.all():
            self.markets[m.id] = m

        self.assets = {}
        for a in db.query(Asset).all():
            self.assets[a.id] = a

        self._get_fee_schedule()

        self.tf = TradeFile()

        begin = time.time()

        q = db.query(Event).filter_by(status='new').\
            order_by(Event.created.asc()).limit(BATCH_SIZE)

        events = q.all()
        if not len(events):
            print('No events.')
            return

        print('Running %d events..' % len(events))

        for e in events:
            self.funcs[e.method](e)

        db.commit()
        self.tf.commit()
        print("Handled %d events in %f seconds." % (len(events), time.time() - begin))


    def event_place_order(self, e):

        # Create new order (add to session at the end)
        new_order = json.loads(e.body)
        for k in ('uuid','created','account_id'):
            new_order[k] = getattr(e, k)
        for k in ('price','amount'):
            new_order[k] = Decimal(new_order[k])
        new_order['balance'] = new_order['amount']
        o = Order(**new_order)

        market = self.markets[o.market_id]

        print("%-12s %6s %-4s %10.4f @ %10.4f   [account_id:%d]\n%s %23.23s %s" % (
            e.method, o.type, o.side, o.amount,
            o.price, e.account_id, e.created, '', e.uuid))

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

        demand = o.amount
        for om in q.all():
            # Fill until demand is empty
            if not demand:
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
            bq = self.session.query(
                Ledger.account_id,
                Ledger.asset_id,
                func.sum(Ledger.amount).label('balance'),
            ).filter(
                Ledger.account_id.in_(accounts),
                Ledger.asset_id.in_((market.asset.id, market.uoa.id))
            ).group_by(Ledger.account_id, Ledger.asset_id)

            bal = {}

            for i in (market.asset.id, market.uoa.id):
                if i not in bal:
                    bal[i] = {}
                for j in accounts:
                    bal[i][j] = 0


            for b in bq.all():
                if b.asset_id not in bal:
                    bal[b.asset_id] = {}
                bal[b.asset_id][b.account_id] = b.balance
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
                order      = o,
                type       = 'taker',
                fee_rate   = Decimal(taker_rate),
                amount     = t.amount if o.side == 'buy' else t.total,
            )
            ms = TradeSide(
                uuid       = shortuuid.uuid(),
                account_id = om.account_id,
                trade      = t,
                order      = om,
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
            sum1 = 0
            for values in ledgers:
                l = Ledger(**dict(zip(keys, values)))
                l.type = 'trade'
                if l.asset_id in bal and l.account_id in bal[l.asset_id]:
                    l.balance = bal[l.asset_id][l.account_id] + l.amount
                self.session.add(l)
                sum1 += int(float(l.amount)*100)
                print("%3s %4d %8.2f" % (
                    self.assets[l.asset_id].symbol, l.account_id, l.amount))
            print('SUM:',sum1)

            # Tee trade stream (todo)
            self.tf.append(market, ','.join([
                t.created.strftime(DT_FORMAT),
                "%.10f" % t.price,
                "%.10f" % t.amount
            ]))

            # New order balance and status
            om.balance = om.balance - tx_amt
            om.status = self.get_order_status(om)

            print("%12s %10.4f  %10.4f @ %10.4f   [account_id:%d]\n   %s %23.23s %s" % (
            'fill', tx_amt, om.balance,
            om.price, om.account_id, om.created, '', e.uuid))


        o.status = self.get_order_status(o)
        self.session.add(o)

        # Set event done
        e.status = 'done'

    def get_order_status(self, obj):
        if obj.balance == 0:
            return 'closed'
        elif obj.balance != obj.amount:
            return 'partial'
        else:
            return 'open'

    def event_cancel_order(self, e):
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

    def event_deposit(self, e):
        p = json.loads(e.body)

        l = model.Ledger(
            account_id=p['account_id'],
            asset_id=p['asset_id'],
            amount=p['amount']
        )
        self.session.add(l)
        e.status = 'done'

    def event_withdraw(self, e):
        p = json.loads(e.body)

        l = model.Ledger(
            account_id=p['account_id'],
            asset_id=p['asset_id'],
            amount=p['amount'] * -1
        )
        self.session.add(l)
        e.status = 'done'


"""
Fees

Trade 5 @ $20.00 (Eric buys)

Method #1 (balanced; fees):            unit of account (balanced)
SHT Shawn   -5.00                      -100.00
USD Eric  -100.00                      -100.00
SHT Eric     4.90 (2% taker: 0.10)       98.00
USD Shawn   99.00 (1% maker: 1.00)       99.00
USD Exchg    1.00                         1.00
SHT Exchg    0.10                         2.00

Method #2 (unbalanced; convert fee):   unit of account (balanced)
SHT Shawn   -5.00                      -100.00
USD Eric  -100.00                      -100.00
SHT Eric     4.90 (2% taker: 0.10)       98.00
USD Shawn   99.00 (1% maker: 1.00)       99.00
USD Exchg    3.00 (convert to USD)        3.00

"""

