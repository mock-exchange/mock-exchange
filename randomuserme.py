#!/usr/bin/env python

import os
from os import path
from pathlib import Path
from urllib.parse import urlparse
import argparse
import csv
import numpy as np
import math
import random
from datetime import datetime, timedelta
import sqlite3
import json
from collections import namedtuple
import requests
import shortuuid
import time

from sqlalchemy import create_engine, and_, or_
from sqlalchemy.orm import Session, joinedload

import model
from mocklib import random_dates, SQL

from decimal import Decimal

#import logging
#logging.basicConfig()
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

ENTITY = {
    'asset': model.Asset,
    'market': model.Market,
    'owner': model.Owner
}

API_ENTITIES = {}

DATA_DIR = 'data'

CSV_OPTS = { 'delimiter': ',', 'quotechar': '"', 'quoting': csv.QUOTE_MINIMAL }

class Main():

    def __init__(self):

        self.engine = create_engine('sqlite:///mockex.db')
        self.session = Session(self.engine)

        parser = argparse.ArgumentParser(description='Import utility')
        parser.add_argument("action", choices=[
            'generate_profiles'
        ], help="Action")

        self.args = parser.parse_args()
        getattr(self, 'cmd_' + self.args.action)()
    
    def cmd_generate_profiles(self):
        img_dir = Path('data/randomuser.me')

        if not path.exists(img_dir):
            os.mkdir(img_dir)

        #self.randomuser_fetchjson()
        #self.randomuser_fetchimages()
        self.randomuser_import2db()


    def randomuser_pages(self):
        return range(1,6)

    def randomuser_fetchjson(self):
        img_dir = Path('data/randomuser.me')

        # 1000 per page, 5 pages
        for page in self.randomuser_pages():
            print("page %s: " % page, end='')

            json_file = img_dir / ('page_' + str(page) + '.json')

            if path.exists(json_file):
                print("already cached")
                continue

            base_url = 'https://randomuser.me/api/1.3/'
            params = {
                'seed': 'foobar',
                'results': '1000',  # 5000,
                'page': str(page),
                'inc': 'gender,name,email,picture,nat,login,dob,location,phone',
                'nat': 'US'
            }
            qs = '&'.join([key+'='+value for key, value in params.items()])
            url = '?'.join([base_url, qs])

            if page > 1:
                print("sleeping 2 seconds..")
                time.sleep(2)

            print("fetching",url)
            r = requests.get(url)
            with open(json_file, 'w') as f:
                f.write(json.dumps(r.json()))
                print("wrote",json_file)


    def randomuser_fetchimages(self):
        img_dir = Path('data/randomuser.me')

        fetch_cnt = 0

        for page in self.randomuser_pages():
            print("page %s: " % page)

            json_file = img_dir / ('page_' + str(page) + '.json')

            if not path.exists(json_file):
                print("not found")
                continue

            with open(json_file) as f:
                json_data = f.read()

            r = json.loads(json_data)

            for i in r['results']:
                imgurl = i['picture']['large']
                to_file = urlparse(imgurl).path[1:]
                imgfile = Path(img_dir, to_file)
                print(imgurl)
                #print(to_file)
                #print(imgfile)
                if path.exists(imgfile):
                    print("already cached")
                    continue
                dirfile = path.dirname(imgfile)
                if not path.exists(dirfile):
                    os.makedirs(dirfile)
               
                if fetch_cnt > 5:
                    fetch_cnt = 0
                    print("sleeping 3 seconds..")
                    time.sleep(3)

                r = requests.get(imgurl, allow_redirects=True)
                open(imgfile, 'wb').write(r.content)
                fetch_cnt += 1


    def randomuser_import2db(self):
        img_dir = Path('data/randomuser.me')

        for page in self.randomuser_pages():
            print("page %s: " % page, end='')

            json_file = img_dir / ('page_' + str(page) + '.json')

            if not path.exists(json_file):
                print("not found")
                continue

            with open(json_file) as f:
                json_data = f.read()

            r = json.loads(json_data)

            for i in r['results']:
                arg = {
                    'name'     : ' '.join((i['name']['first'], i['name']['last'])),
                    'email'    : i['email'],
                    'username' : i['login']['username'],
                    'uuid'     : shortuuid.uuid(i['login']['uuid']),
                    'profile'  : json.dumps(i)
                }
                
                imgurl = i['picture']['large']
                arg['picture'] = '/static' + urlparse(imgurl).path


                print(arg['name'], arg['username'])
                print()
              
                # Crappy replace into
                o = self.session.query(model.Owner).filter_by(
                    username=arg['username']).one_or_none()
                u = self.session.query(model.Owner).filter_by(
                    email=arg['email']).one_or_none()

                # Some emails aren't unique
                if u:
                    arg['email'] = arg['username'] + '@foobar.com'

                if o:
                    print('update..', o)
                    for a in arg:
                        setattr(o, a, arg[a])
                else:
                    print('insert..')
                    o = model.Owner(**arg)
                    self.session.add(o)
                self.session.commit()
           
            self.session.commit()



if __name__ == '__main__':
    Main()




