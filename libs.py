import csv
from datetime import datetime, timedelta, time
import numpy as np
import math
from random import randrange, randint

from sqlalchemy import create_engine, and_, or_
from sqlalchemy.orm import Session

import model

ENTITY = {
    'asset': model.Asset,
    'market': model.Market,
    'owner': model.Owner
}

DATA_DIR = 'data'

#CSV_OPTS = { 'delimiter': ',', 'quotechar': '"', 'quoting': csv.QUOTE_MINIMAL }


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


