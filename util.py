#!/usr/bin/env python

import argparse
import csv

from sqlalchemy import create_engine
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
            'import', 'export'
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



if __name__ == '__main__':
    Main()


