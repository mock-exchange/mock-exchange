import csv
from datetime import datetime, timedelta, time
#import numpy as np
import math
from random import randrange, randint
import os
import re
from pathlib import Path

from sqlalchemy import create_engine, and_, or_
from sqlalchemy.orm import Session

import model

ENTITY = {
    'asset': model.Asset,
    'market': model.Market,
    'account': model.Account
}

DATA_DIR = 'data'

DT_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

#CSV_OPTS = { 'delimiter': ',', 'quotechar': '"', 'quoting': csv.QUOTE_MINIMAL }

# Let's grab our SQL from outside so it's easier to manage
#SQL_ROOT = os.path.dirname(app.instance_path) + '/sql/'
SQL_ROOT = 'sql'
SQL = {}

for filename in os.listdir(SQL_ROOT):
  if re.search(r'\.sql$', filename):
    name = os.path.splitext(filename)[0]
    with open(SQL_ROOT + '/' + filename) as f:
      SQL[name] = f.read()
      f.close()


intervals = ['1m','5m','15m','1h','6h','1d']

def random_dates(count, start=None, end=None):
    if not start:
        start = datetime.utcnow() - timedelta(days=30)
    if not end:
        end = datetime.utcnow()

    time_between_dates = end - start
    days_between_dates = time_between_dates.days

    dates = []
    for i in range(count):
        random_number_of_days = randrange(days_between_dates)
        date = start + timedelta(days=random_number_of_days)

        hour = randint(0,23)
        minute = randint(0,59)
        second = randint(0,59)
        
        tm = time(hour, minute, second)
        dates.append(datetime.combine(date, tm))

    return sorted(dates)


TRADE_DIR = Path('data/last_trades')

if not os.path.exists(TRADE_DIR):
    os.makedirs(TRADE_DIR)

class TradeFile():
    def __init__(self):
        self.data = {}

    def append(self, market_id, row):
        if market_id not in self.data:
            self.data[market_id] = []
        self.data[market_id].append(row)
        self.data[market_id] = self.data[market_id][-30:]

    def get(self, market_id):
        to_path = TRADE_DIR / (str(market_id) + '.csv')
        data = []
        with open(to_path) as r:
            for row in r.read().splitlines():
                (dt, price, amount) = row.split(',')
                data.append({
                    'created': dt,
                    'price': price,
                    'amount': amount
                })
        return list(reversed(data))

    def commit(self):
        ids = list(self.data.keys())
        for market_id in ids:
            to_path = TRADE_DIR / (str(market_id) + '.csv')
            to_tmp = str(to_path) + '.tmp'
            with open(to_path) as r:
                data = r.read().splitlines()
                self.data[market_id] = data + self.data[market_id]
                self.data[market_id] = self.data[market_id][-30:]

            with open(to_tmp, "w") as f:
                f.write("\n".join(self.data[market_id]))
                f.flush()
                os.fsync(f.fileno())
                del self.data[market_id]
                os.rename(to_tmp, to_path)


