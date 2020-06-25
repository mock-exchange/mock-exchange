import os
from pathlib import Path
from datetime import datetime, timedelta
import json
import shortuuid
import time
from collections import namedtuple
from decimal import Decimal

from sqlalchemy import create_engine, and_, or_, func
from sqlalchemy.orm import Session, joinedload

import model
from config import SQL, DT_FORMAT
from model import (
    Account, Market, Asset, Event, Order, Trade, TradeSide, Ledger
)
from lib import random_dates, TradeFile

BATCH_SIZE = 1000

MAKER_FEE_RATE = Decimal(0.16 / 100)
TAKER_FEE_RATE = Decimal(0.26 / 100)
FEE_ACCOUNT_ID = 1

""" Methods to record fees

Method #1 - Separate ledgers
    ledger: fee account, +fee
    ledger: fee account, +fee
    ledger: buyer, -fee
    ledger: seller, -fee

Method #2 - Separate ledgers only for fee account

Method #3 - Attribute on existing ledger entries

"""

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

        #db.commit()
        #self.tf.commit()
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

        print("%-12s %6s %-4s %10.4f @ %10.4f  %s\n%50.50s%s" % (
            e.method, o.type, o.side, o.amount,
            o.price, e.uuid, '',e.created))

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

            tx_amt = om.balance if demand > om.balance else demand
            tx_total = tx_amt * om.price
            demand -= tx_amt

            taker_fee = tx_amt * TAKER_FEE_RATE
            maker_fee = tx_amt * MAKER_FEE_RATE
            tx_subamt = tx_amt - taker_fee - maker_fee

            t = Trade(
                uuid        = shortuuid.uuid(),
                created     = e.created,
                market_id   = market.id,
                price       = om.price,
                amount      = tx_amt,
            )

            # fee comes out of both sides
            """
            ts = TradeSide(
                trade_uuid = t.uuid,
                account_id = o.account_id,
                order_uuid = o.uuid,
                sub_amount =    # total amount
                fee =           # fee
                amount =        # total - fee
            )
            ms = TradeSide(
                trade_uuid = t.uuid,
                account_id = om.account_id,
                order_uuid = om.uuid,
                total = 0,
                fee = maker_fee,
                amount = 0
            )
            """

            self.session.add(t)
            #for x in (t, ts, ms):
            #    self.session.add(x)
            """
            Buy 10 @ 21.50 -> Sell 5 @ 21.50
            Trade:
                price:       21.50
                amount:      5
                total:       107.50  (price * amount)

            Side 1:
                trade:        -> (pointer)
                fee_rate:     .0026 (taker)
                amount:       t.amount - (fee_rate * t.total)
                total:        t.total - (fee_rate * t.total)

            Side 2:
                trade:        -> (pointer)
                fee_rate:     .0016 (maker)
                amount:       t.amount - (fee_rate * t.total)
                total:        t.total - (fee_rate * t.total)
            """

            buyer_id  = e.account_id if o.side == 'buy' else om.account_id
            seller_id = e.account_id if o.side == 'sell' else om.account_id

            # Query balance to update running balance in ledger
            bq = self.session.query(
                Ledger.account_id,
                Ledger.asset_id,
                func.sum(Ledger.amount).label('balance'),
            ).filter(
                Ledger.account_id.in_((buyer_id, seller_id, FEE_ACCOUNT_ID)),
                Ledger.asset_id.in_((market.asset.id, market.uoa.id))
            ).group_by(Ledger.account_id, Ledger.asset_id)

            bal = {}
            for b in bq.all():
                if b.asset_id not in bal:
                    bal[b.asset_id] = {}
                bal[b.asset_id][b.account_id] = b.balance

            keys = ['asset_id','account_id','amount']
            ledgers = [
                (market.asset.id,  seller_id, tx_amt * -1),
                (market.asset.id,  buyer_id,  tx_amt),
                (market.uoa.id,    buyer_id,  tx_total * -1),
                (market.uoa.id,    seller_id, tx_total),

                # Fee account
                (market.uoa.id,    FEE_ACCOUNT_ID, maker_fee),
                (market.uoa.id,    FEE_ACCOUNT_ID, taker_fee),
            ]

            # Create ledger entries
            for values in ledgers:
                l = Ledger(**dict(zip(keys, values)))
                if l.asset_id in bal and l.account_id in bal[l.asset_id]:
                    l.balance = bal[l.asset_id][l.account_id] + l.amount
                self.session.add(l)



            # Tee trade stream (todo)
            self.tf.append(market, ','.join([
                t.created.strftime(DT_FORMAT),
                "%.10f" % t.price,
                "%.10f" % t.amount
            ]))

            # New order balance and status
            om.balance = om.balance - tx_amt
            om.status = self.get_order_status(om)

            print("%12s %10.4f  %10.4f @ %10.4f  %s\n%50.50s%s" % (
                'fill', tx_amt, om.balance,
                om.price, om.uuid, '', om.created))

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

