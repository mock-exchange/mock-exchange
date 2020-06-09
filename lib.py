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

from config import CACHE_DIR

import model


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


class TradeFile():
    def __init__(self):
        self.data = {}

    def append(self, m, row):
        if m not in self.data:
            self.data[m] = []
        self.data[m].append(row)
        self.data[m] = self.data[m][-30:]

    def get(self, m):
        to_path = CACHE_DIR / m.code / 'last_trades.csv'
        data = []
        if os.path.exists(to_path):
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
        for m in ids:
            to_path = CACHE_DIR / m.code / 'last_trades.csv'
            to_tmp = str(to_path) + '.tmp'
            to_dir = os.path.dirname(to_path)

            if not os.path.exists(to_dir):
                os.makedirs(to_dir)

            if os.path.exists(to_path):
                with open(to_path) as r:
                    data = r.read().splitlines()
                    self.data[m] = data + self.data[m]
                    self.data[m] = self.data[m][-30:]

            with open(to_tmp, "w") as f:
                f.write("\n".join(self.data[m]))
                f.flush()
                os.fsync(f.fileno())
                del self.data[m]
                os.rename(to_tmp, to_path)


