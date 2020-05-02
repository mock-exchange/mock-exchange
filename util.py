#!/usr/bin/env python

import argparse
import csv

from sqlalchemy import create_engine, and_, or_
from sqlalchemy.orm import Session

import model

ENTITY = {
    'asset': model.Asset,
    'market': model.Market
}

DATA_DIR = 'data'

CSV_OPTS = { 'delimiter': ',', 'quotechar': '"', 'quoting': csv.QUOTE_MINIMAL }

class Main():

    def __init__(self):

        engine = create_engine('sqlite:///me.db')
        self.session = Session(engine)

        parser = argparse.ArgumentParser(description='Import utility')
        parser.add_argument("action", choices=[
            'import', 'export',
            'ordproc'
        ], help="Action")

        self.args = parser.parse_args()

        getattr(self, 'cmd_' + self.args.action)()
    
    def cmd_export(self):
        for e in ENTITY.keys():
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
        for e in ENTITY.keys():
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
            print("--- %05d %8s %-4s %10d %10d" % (
            o.id, o.type, o.direction, o.amount, o.price))
            
            # Where
            where = []

            # status IN (open, partial)
            # AND direction == opposite
            
            # ORDER BY
            #   PRICE ASC,
            #   ID ASC -- FIFO

            where.append(model.Order.status.in_(['open','partial']))

            newdir = 'sell' if o.direction == 'buy' else 'buy'
            where.append(model.Order.direction==newdir)

            q2 = self.session.query(
                model.Order
            ).filter(
                and_(*where)
            ).order_by(
                model.Order.price.asc(),
                model.Order.id.asc()
            )
            for o2 in q2.all():
                print("  ? %05d %8s %-4s %10d %10d" % (
                o2.id, o2.type, o2.direction, o2.amount, o2.price))

            # If no matches, add to book (by changing status to open)
            #o.status = 'open'
            #self.session.commit()

if __name__ == '__main__':
    Main()


