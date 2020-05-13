#!/usr/bin/env python

import argparse
import csv
import numpy as np
import math
import random
from datetime import datetime, timedelta
import sqlite3
import json
from collections import namedtuple

from sqlalchemy import create_engine, and_, or_
from sqlalchemy.orm import Session, joinedload

import model
from libs import random_dates, SQL

#import logging
#logging.basicConfig()
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

ENTITY = {
    'asset': model.Asset,
    'market': model.Market,
    'owner': model.Owner
}

DATA_DIR = 'data'

CSV_OPTS = { 'delimiter': ',', 'quotechar': '"', 'quoting': csv.QUOTE_MINIMAL }

class Main():

    def __init__(self):

        self.engine = create_engine('sqlite:///me.db')
        self.session = Session(self.engine)

        parser = argparse.ArgumentParser(description='Import utility')
        parser.add_argument("action", choices=[
            'import', 'export',
            'book',
            'events', 'randbook',
            'trade', 'test',
            'ordertest'
        ], help="Action")

        parser.add_argument("--entity",  help="entity")

        self.args = parser.parse_args()
        #print(self.args)
        getattr(self, 'cmd_' + self.args.action)()
    
    def cmd_ordertest(self):
        print('ordertest')
        
        
        q = self.session.query(model.Order)
        #.options(joinedload(model.Order.market))
        
        for r in q.filter(model.Order.owner==1):
            print(r.id, r.market.name, r.owner, r.price, r.amount)

    def cmd_book(self):
        print('book')
        sql = SQL['book']

        con = self.engine.connect()
        rs = con.execute(sql, (1,1,))
        print("%-10s %10s %10s %10s" % (
        'Side', 'Price','Amount','Total'))

        for row in rs:
            print("%-10s %10.2f %10.2f %10.2f" % tuple(row))


    def cmd_test(self):
        print('test')
        print('sqlite3.version:',sqlite3.version)
        print('sqlite3.sqlite_version:',sqlite3.sqlite_version)
        conn = sqlite3.connect('me.db')
        c = conn.cursor()
        sql = """
        select
            distinct date(created),
            first_value(price) over w as open,
            max(price) over w as high,
            min(price) over w as low,
            last_value(price) over w as close,
            CAST(sum(amount) over w AS INT) as volume
            from trade
            where
                market = 3
            window w as (partition by date(created))
        """
        print(sql)
        c.execute(sql)
        print("%-10s %10s %10s %10s %10s %10s" % (
        'Date', 'High','Low','Open','Close','Volume'))

        for row in c.fetchall():
            print("%10s %10.2f %10.2f %10.2f %10.2f %s" % (row))

    def cmd_export(self):
        ser = ENTITY.keys() if 'all' == self.args.entity else [self.args.entity]
        for e in ser:
            print('Export',e,'.. ', end='')
            Entity = ENTITY[e]
            q = self.session.query(Entity)
            file = DATA_DIR + '/' + e + '.csv'
            with open(file, 'w') as csvfile:
                writer = csv.writer(csvfile, **CSV_OPTS)
                header = Entity.__table__.columns.keys()

                writer.writerow(header)
                
                cnt = 0
                for record in q.all():
                    writer.writerow([getattr(record, c) for c in header ])
                    cnt += 1
                print(cnt, 'rows')


    def cmd_import(self):
        ser = ENTITY.keys() if 'all' == self.args.entity else [self.args.entity]
        for e in ser:
            print('Import',e,'.. ', end='')
            Entity = ENTITY[e]
            file = DATA_DIR + '/' + e + '.csv'
            with open(file) as csvfile:
                reader = csv.DictReader(csvfile, **CSV_OPTS)

                # Clean table
                deleted = self.session.query(Entity).delete()
                self.session.commit()

                cnt = 0
                for row in reader:
                    self.session.add(Entity(**row))
                    cnt += 1
                self.session.commit()
                print(cnt, 'rows imported')

    def cmd_trade(self):
        file = DATA_DIR + '/bitmex_trades_2020-04-01_XBTUSD.csv'
        with open(file) as csvfile:
            reader = csv.DictReader(csvfile, **CSV_OPTS)

            # Clean table
            #deleted = self.session.query(model.Trade).delete()
            #self.session.commit()

            skip = 0
            for row in reader:
                skip += 1
                if skip > 100000:
                    break

            start = datetime.utcnow() - timedelta(days=120)
            dt = start
            cnt = 0
            for row in reader:
                #fuck = int(row['timestamp'])
                #print(fuck)
                #fuck = fuck / 1000000
                #print(fuck)
                #dt = datetime.utcfromtimestamp(fuck)
                dt = dt + timedelta(minutes=5)
                print(dt)
                price = float(row['price']) - 6000
                amount = int(row['amount']) * .001
                m = model.Trade(market=3, created=dt, price=price, amount=amount)
                print(m.__dict__)
                self.session.add(m)
                cnt += 1
                if cnt % 1000 == 0:
                    self.session.commit()

                if cnt > 50000:
                    break
                
            self.session.commit()
            print(cnt, 'rows imported')


    def cmd_randbook(self):
        market = 1
        market_rate = 8800

        owner = []
        owner_idx = {}
        for o in self.session.query(model.Owner).all():
            owner_idx[o.id] = o.name
            owner.append(o.id)

        assets = {
            'usd': {
                'issue': 100000000,
                'id': 1
            },
            'btc': {
                'issue': 50000,
                'id': 2
            }
        }

        """
        # Delete first
        self.session.query(model.Account).delete()
        self.session.commit()

        # Create accounts for all owners.
        # Set initial balance from pareto distribution of issue amount.
        shape = 5
        size = len(owner)
        dist = np.random.pareto(shape, size)

        dist_sum = sum(dist)
        for i, d in enumerate(dist):
            dist_rate =  d / dist_sum
            
            for asset in assets.keys():
                bal = assets[asset]['issue'] * dist_rate
                bal = math.ceil(bal)
                idx = assets[asset]['id']
                a = model.Account(owner=owner[i], asset=idx, balance=bal)
                self.session.add(a)

        self.session.commit()
        """
        
        print('random dates:')
        dates = random_dates(10000)
        
        """
        ass = {}
        for i in dates:
            match = 'DUP' if i in ass else ''
            ass[i] = 1
            print(i, match)
        return
        """
        # Create orders

        # Delete first
        #self.session.query(model.Order).delete()
        #self.session.commit()


        q = self.session.query(model.Account)
        cnt = 0
        for r in q.filter(model.Account.asset==1):
            print(r.__dict__)
            price = random.randrange(8801,9200)
            #price = random.randrange(8400,8800)
            #amt = r.balance / price 
            o = model.Order(
                created=dates[cnt],
                market=1,
                owner=r.owner,
                price=price,
                direction="sell",
                #direction="buy",
                amount=r.balance,
                amount_left=r.balance
                #amount=amt,
                #amount_left=amt
            )
            print(o.__dict__)
            #break
            self.session.add(o)
            if cnt % 100 == 0:
                print('commit()')
                self.session.commit()
            cnt += 1

        self.session.commit()


    def add_order(self):
        pass

    def cmd_events(self):
        print('Process events..')

        # 1. Foreach order with new status
        #   a. For limit
        #       A. Foreach order where()  orderby(price)
        #  opposite direction (buy vs sell)
        #  and price <=> our price

        q = self.session.query(
            model.Event
        ).filter_by(
            status='new'
        ).order_by(model.Event.id)

        for e in q.all():
            print("%05d %s %8d %5s\n%s" % (
            e.id, e.created, e.owner, e.action, e.payload))
            #o = json.loads(e.payload)
            #o = type("JSON", (), json.loads(e.payload))()

            
            if e.action == 'co':
                p = json.loads(e.payload)
                order_id = p['order_id']
                o = self.session.query(model.Order).get(order_id)
                print(o.__dict__)
                o.status = 'canceled'

                e.status = 'done'
                self.session.commit()
                continue

            # event add_order
            #if e.action == 'ao':
            #    self.add_order()
            o = json.loads(e.payload, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
            demand = float(o.amount)

            # Where
            where = []
            order = []

            where.append(model.Order.status.in_(['open','partial']))

            if o.direction == 'sell':
                where.append(model.Order.direction == 'buy')
                where.append(model.Order.price >= o.price)

                order.append(model.Order.price.desc())
                order.append(model.Order.id.asc())

            elif o.direction == 'buy':
                where.append(model.Order.direction == 'sell')
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
                print("  > %05d %8s %8s %-4s %10d %10d [ %10d ]" % (
                o2.id, o2.type, o2.status, o2.direction, o2.amount_left, o2.price, demand))
                # demand 3
                # has 2: tx for 2, 3-2, 1 demand left
                # has 12: tx for 1, 1-1, 0 demand left
                
                # tx is up to demand amt
                if not demand:
                    break
                
                tx_amt = o2.amount_left if demand > o2.amount_left else demand
                demand -= tx_amt
                xx = model.TransactionItem(
                    account=1,
                    amount=tx_amt
                )
                print(xx.__dict__)
                self.session.add(xx)

                yy = model.Trade(
                    market=1,
                    price=o2.price,
                    amount=tx_amt
                )
                self.session.add(yy)

                # then update remaining order amount
                o2.amount_left = o2.amount_left - tx_amt
                o2.status = self.get_status(o2)
                print("  X %05d %8s %8s %-4s %10d %10d [ %10d ]" % (
                o2.id, o2.type, o2.status, o2.direction, o2.amount_left, o2.price, demand))
                print()
            

            if float(demand) == float(0):
                status = 'closed'
            elif float(demand) != float(o.amount):
                status = 'partial'
            else:
                status = 'open'

            no2 = model.Order(
                owner=o.owner,
                market_id=o.market,
                direction=o.direction,
                price=o.price,
                amount=o.amount,
                amount_left=demand,
                status=status
            )
            #no.status = self.get_status(no)
            self.session.add(no2)

            print("new order:")
            print(no2.__dict__)

            e.status = 'done'
            #print("-d- %05d %8s %8s %-4s %10d %10d" % (
            #o.id, o.type, o.status, o.direction, o.amount_left, o.price))
            #print("%05d %s %8d %5s %s" % (
            #e.id, e.created, e.owner, e.action, e.payload))

            self.session.commit()

    def get_status(self, obj):
        if obj.amount_left == 0:
            return 'closed'
        elif obj.amount_left != obj.amount:
            return 'partial'
        else:
            return 'open'


if __name__ == '__main__':
    Main()




