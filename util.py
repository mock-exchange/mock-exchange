#!/usr/bin/env python

import argparse
import csv

from sqlalchemy import create_engine, and_, or_
from sqlalchemy.orm import Session

import model

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
            'ordproc', 'randbook'
        ], help="Action")

        parser.add_argument("--entity",  help="entity")

        self.args = parser.parse_args()
        #print(self.args)
        getattr(self, 'cmd_' + self.args.action)()
    
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

    def cmd_randbook(self):
        owners = 3
        
        market = 1
        market_rate = 8800

        for i in range(owners):
            a = model.Owner()
            self.session.add(a)
            print(a.id)

        self.session.commit()


    def cmd_ordproc(self):
        print('Process orders..')

        # 1. Foreach order with new status
        #   a. For limit
        #       A. Foreach order where()  orderby(price)
        #  opposite direction (buy vs sell)
        #  and price <=> our price

        q = self.session.query(
            model.Order
        ).filter_by(
            status='new'
        ).order_by(model.Order.id)

        for o in q.all():
            print("--- %05d %8s %8s %-4s %10d %10d" % (
            o.id, o.type, o.status, o.direction, o.amount_left, o.price))
            
            # Where
            where = []
            order = []
            # status IN (open, partial)

            # if direction == 'sell'
            #   AND direction == 'buy'
            #   AND price >= this.price -- not lt our limit

            # elif direction == 'buy'
            #   AND direction == 'sell'
            #   AND price <= this.price -- not gt our limit

            # ORDER BY
            #   PRICE ASC,
            #   ID ASC -- FIFO

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
            demand = o.amount_left
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
                    amount=tx_amt,
                    order=o2.id
                )
                print(xx.__dict__)
                self.session.add(xx)
                # then update remaining order amount
                o2.amount_left = o2.amount_left - tx_amt
                o2.status = self.get_status(o2)
                print("  X %05d %8s %8s %-4s %10d %10d [ %10d ]" % (
                o2.id, o2.type, o2.status, o2.direction, o2.amount_left, o2.price, demand))
                print()
            
            o.amount_left = demand
            o.status = self.get_status(o)

            print("-d- %05d %8s %8s %-4s %10d %10d" % (
            o.id, o.type, o.status, o.direction, o.amount_left, o.price))
 
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


