#!/usr/bin/python3

import argparse
import sqlite3

from sqlalchemy import (create_engine, and_, or_)
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import dialects, func, update

import model
from model import dict_factory


def do_order(self):
    pass
    # If order matches, execute it
    # else, add to order book

def add_order(self):
    pass

def cancel_order(self):
    pass


def get_orders(self):
    pass

def get_txs(self):
    pass



class Main():

    def __init__(self):

        conn = sqlite3.connect('test.db')
        conn.row_factory = dict_factory
        db = conn.cursor()

        parser = argparse.ArgumentParser(description='Match engine')
        parser.add_argument("action", choices=[
            'add','cancel','list','initdb'
        ], help="Action")


        self.args = parser.parse_args()

        print('args:',self.args)

        getattr(self, 'cmd_' + self.args.action)()
    
    def cmd_initdb(self):
        print('Setup db.. ', end='')
        engine = create_engine('sqlite:///test.db')
        model.Base.metadata.create_all(engine)
        session = Session(engine)
        print('done')

    def cmd_add(self):
        print('add')

    def cmd_cancel(self):
        print('cancel')

    def cmd_list(self):
        print('list')

if __name__ == '__main__':
    Main()


