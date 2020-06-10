import os
from pathlib import Path
from datetime import datetime, timedelta
import json
#import shortuuid
import time
from collections import namedtuple
from decimal import Decimal

from sqlalchemy import create_engine, and_, or_, func
from sqlalchemy.orm import Session, joinedload

import model
from config import SQL, DT_FORMAT
from model import (Account, Market, Asset, Event, Order, Trade, Ledger)
from lib import random_dates, TradeFile

BATCH_SIZE = 1000

class EventTask():
    def __init__(self):
        pass

"""
class PlaceOrder(EventTask)
class CancelOrder(EventTask)
class Deposit(EventTask)
class Withdraw(EventTask)
"""

class EventRunner():
    def __init__(self, session):
        self.session = session

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

        q2 = db.query(Event).filter_by(status='new').\
            order_by(Event.created.asc()).limit(BATCH_SIZE)

        events = q2.all()
        if not len(events):
            print('No events.')
            return

        print('Running %d events..' % len(events))

        for e in events:
            print("%05d %d %s %8s\n%s" % (
            e.id, e.account_id, e.created, e.method, e.body))
            #o = json.loads(e.payload)
            #o = type("JSON", (), json.loads(e.payload))()

            # Event(e).execute()
            # call(e, body, market)
            if e.method == 'deposit':
                self.event_deposit(e)
            elif e.method == 'withdraw':
                self.event_withdraw(e)
            elif e.method == 'cancel-order':
                self.event_cancel_order(e)
            elif e.method == 'place-order':
                self.event_place_order(e)

        db.commit()
        self.tf.commit()
        print("Handled %d events in %f seconds." % (len(events), time.time() - begin))

    def get_body(self, e):
        return json.loads(e.body, object_hook=lambda d: namedtuple('eventBody', d.keys())(*d.values()))

    def event_place_order(self, e):
        o = self.get_body(e)
        demand = Decimal(o.amount)

        market = self.markets[o.market_id]

        # Where
        where = []
        order = []

        where.append(model.Order.market_id == o.market_id)

        where.append(model.Order.status.in_(['open','partial']))

        # NO WASH TRADES
        # Exclude account's orders from set to be matched
        where.append(model.Order.account_id != e.account_id)


        if o.side == 'sell':
            where.append(model.Order.side == 'buy')
            where.append(model.Order.price >= o.price)

            order.append(model.Order.price.desc())
            order.append(model.Order.id.asc())

        elif o.side == 'buy':
            where.append(model.Order.side == 'sell')
            where.append(model.Order.price <= o.price)

            order.append(model.Order.price.asc())
            order.append(model.Order.id.asc())

        # This query returns book matches, so start slicing thru them
        q2 = self.session.query(
            model.Order
        ).filter(
            and_(*where)
        ).order_by(
            *order
        )
        for o2 in q2.all():
            print("  > %05d %8s %8s %-4s %10d %10d [ %10d ] %s" % (
            o2.id, o2.type, o2.status, o2.side, o2.balance, o2.price, 
            demand, o2.market_id))
            # demand 3
            # has 2: tx for 2, 3-2, 1 demand left
            # has 12: tx for 1, 1-1, 0 demand left
            
            # tx is up to demand amt
            if not demand:
                break
            
            tx_amt = o2.balance if demand > o2.balance else demand
            tx_total = tx_amt * o2.price
            demand -= tx_amt


            # TODO: 2 Ledger entries for each side of the trade
            # o.type == 'sell'
            # asset1 -amount o.account_id (me)
            # asset1 +amount o2.account_id (buyer)
            # uoa    -amount o2.account_id (buyer)
            # uoa    +amount o.account_id (me)

            # o.type == 'buy'
            # asset1 -amount o2.account_id (seller)
            # asset1 +amount o.account_id (me)
            # uoa    -amount o.account_id (me)
            # uoa    +amount o2.account_id (seller)
            
            buyer_id  = e.account_id if o.side == 'buy' else o2.account_id
            seller_id = e.account_id if o.side == 'sell' else o2.account_id

            bq = self.session.query(
                Ledger.account_id,
                Ledger.asset_id,
                func.sum(Ledger.amount).label('balance'),
            ).filter(
                Ledger.account_id.in_((buyer_id, seller_id)),
                Ledger.asset_id.in_((market.asset.id, market.uoa.id))
            ).group_by(Ledger.account_id, Ledger.asset_id)

            bal = {}
            for b in bq.all():
                if b.asset_id not in bal:
                    bal[b.asset_id] = {}
                bal[b.asset_id][b.account_id] = b.balance

            """
            sht eric -amt   sht.eric.bal + -amt
            sht joe   amt   sht.joe.bal  +  amt
            usd joe  -amt   usd.joe.bal  + -amt
            usd eric  amt   usd.eric.bal +  amt
            """

            keys = ['asset_id','account_id','amount']
            ledgers = [
                (market.asset.id,  seller_id, tx_amt * -1),
                (market.asset.id,  buyer_id,  tx_amt),
                (market.uoa.id,    buyer_id,  tx_total * -1),
                (market.uoa.id,    seller_id, tx_total)
            ]
            
            print("Ledgers")
            for values in ledgers:
                l = Ledger(**dict(zip(keys, values)))
                if l.asset_id in bal and l.account_id in bal[l.asset_id]:
                    l.balance = bal[l.asset_id][l.account_id] + l.amount
                self.session.add(l)
                
                lout = dict(l.__dict__)
                lout.pop('_sa_instance_state', None)
                print(lout)

            """
            OrderA        OrderB
             \              /
              --------------
                    |
                   / \
             TradeA   TradeB
                   \ /
                    |
                  Trade (anonymous)

                        TradeAccount(
                            trade_id   =
                            account_id = 
                            asset_id   =
                            amount     =
                        )

            Eric sells 2 BTC to Shawn @ $8,000, total $16,000


            Shawn buys 2 BTC from Shawn @ $8,000, total $16,000


            """

            yy = model.Trade(  # Anonymous
                account_id  = e.account_id,
                market_id   = market.id,
                price       = o2.price,
                amount      = tx_amt,
                created     = e.created
            )
            self.session.add(yy)
            self.tf.append(market, ','.join([
                yy.created.strftime(DT_FORMAT),
                "%.10f" % yy.price,
                "%.10f" % yy.amount
            ]))
            # then update remaining order amount
            o2.balance = o2.balance - tx_amt
            o2.status = self.get_status(o2)
            print("  X %05d %8s %8s %-4s %10d %10d [ %10d ]" % (
            o2.id, o2.type, o2.status, o2.side, o2.balance, o2.price, demand))
            print()
        

        if Decimal(demand) == Decimal(0):
            status = 'closed'
        elif Decimal(demand) != Decimal(o.amount):
            status = 'partial'
        else:
            status = 'open'

        no2 = model.Order(
            account_id=e.account_id,
            market_id=o.market_id,
            side=o.side,
            type='limit',
            price=o.price,
            amount=o.amount,
            balance=demand,
            status=status,
            uuid=e.uuid,
            created=e.created
        )
        #no.status = self.get_status(no)
        self.session.add(no2)

        print("new order:")
        print(no2.__dict__)

        e.status = 'done'
        #print("-d- %05d %8s %8s %-4s %10d %10d" % (
        #o.id, o.type, o.status, o.direction, o.amount_left, o.price))
        #print("%05d %s %8d %5s %s" % (
        #e.id, e.created, e.account, e.action, e.payload))

        #self.session.commit()


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
        #self.session.commit()

    def event_deposit(self, e):
        p = json.loads(e.body)

        l = model.Ledger(
            account_id=p['account_id'],
            asset_id=p['asset_id'],
            amount=p['amount']
        )
        self.session.add(l)
        e.status = 'done'
        #self.session.commit()

    def event_withdraw(self, e):
        p = json.loads(e.body)

        l = model.Ledger(
            account_id=p['account_id'],
            asset_id=p['asset_id'],
            amount=p['amount'] * -1
        )
        self.session.add(l)
        e.status = 'done'
        #self.session.commit()


    def get_status(self, obj):
        if obj.balance == 0:
            return 'closed'
        elif obj.balance != obj.amount:
            return 'partial'
        else:
            return 'open'


