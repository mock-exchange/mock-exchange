#!/usr/bin/env python

import argparse
import sqlite3
import sys

from sqlalchemy import (create_engine, and_, or_)
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import dialects, func, update

import model
from model import (dict_factory, Tx)

print('sys.path:\n' + '\n'.join(sys.path))

def add_order(session):
    tx = Tx(
        src_account_id=1,
        dst_account_id=2,
        price=100,
        amount=2
    )
    session.add(tx)
    session.commit()


def cancel_order(self):
    pass


def get_orders(self):
    pass

def get_txs(self):
    pass



class Main():

    def __init__(self):

        conn = sqlite3.connect('me.db')
        conn.row_factory = dict_factory
        db = conn.cursor()

        engine = create_engine('sqlite:///me.db')
        self.session = Session(engine)

        parser = argparse.ArgumentParser(description='Match engine')
        """
        parser.add_argument("action", choices=[
            'add','cancel','list','initdb'
        ], help="Action")
        """
        subparsers = parser.add_subparsers(help='sub-command help',
            dest='action')

        parser_initdb = subparsers.add_parser('initdb', help='init db')

        parser_add = subparsers.add_parser('add', help='add help')
        #parser_add.add_argument('bar', type=int, help=' help')
        #parser_add.add_argument('--fuck', action='store_false', help='fuck me')
        #parser.add_argument('--media', action='store_false')

        parser_list = subparsers.add_parser('list', help='list [entity]')
        parser_list.add_argument('entity', choices=['account', 'tx'], 
            help='entity')
        #parser_list.add_argument('--oldest', action='store_true',
        #    help='oldest first')

        self.args = parser.parse_args()

        print('args:',self.args)

        getattr(self, 'cmd_' + self.args.action)()
    
    def cmd_initdb(self):
        print('Setup db.. ', end='')
        engine = create_engine('sqlite:///me.db')
        model.Base.metadata.create_all(engine)
        session = Session(engine)
        print('done')

    def cmd_add(self):
        print('add')
        add_order(self.session)

    def cmd_cancel(self):
        print('cancel')

    def cmd_list(self):
        print('list')

if __name__ == '__main__':
    Main()


