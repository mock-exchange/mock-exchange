import arrow
import os
from pathlib import Path
import json
import humanize
import re
from datetime import datetime, timedelta
import math
import types

from sqlalchemy import create_engine, and_, or_, func
from sqlalchemy.orm import Session, joinedload

from model import (Account, Market, Asset, Event, Order, Trade, Ledger)
from lib import SQL

OUT_DIR = Path('data/ohlc')

INTERVALS = ('1m','5m','15m','1h','6h','1d')

ATTR_WORD = {
    'm': 'minutes',
    'h': 'hours',
    'd': 'days'
}

INTERVAL_PARTS = {}
for i in INTERVALS:
    (num, attr) = re.match("(\d+)([mhd])",i).groups()
    INTERVAL_PARTS[i] = [
        int(num),
        attr
    ]

FRAME_COUNT = 500

"""
/1d/YYYY        - query by Y (365 rows) more?
/6h/YYYY/Q      - query by Q (360 rows) more?
/1h/YYYY/MM     - query by M (720 rows)
/15m/YYYY/WW    - query by W (672 rows) week of year
/5m/YYYY/MMDD   - query by D (280 rows)
/1m/YYYY/MMDDHH - query by H (60 rows) I guess by hour
(smaller INTERVALS only seem useful for real time watching)

 1d/YYYY/YYYY.json
 6h/YYYY/QQ/YYYY-QQ.json
 1h/YYYY/MM/YYYY-MM.json
15m/YYYY/WW/YYYY-WW.json
 5m/YYYY/MM/DD/YYYY-MM-DD.json
 1m/YYYY/MM/DD/HH/YYYY-MM-DDTHH.json
"""

INTERVAL_AGGREGATE = {
    '1d'  : ('year',    '%Y'),
    '6h'  : ('quarter', lambda dt: dt.strftime('%Y/Q') + str(math.ceil(dt.month/3))),
    '1h'  : ('month',   '%Y/%m'),
    '15m' : ('week',    '%Y/%W'),
    '5m'  : ('day',     '%Y/%m/%d'),
    #'1m'  : ('hour',    '%Y/%m/%d/%H'),
}

class OHLC:
    def __init__(self, session):
        self.db = session

    def get_date_range(self, interval):

        (num, attr) = INTERVAL_PARTS[interval]
        end = datetime.utcnow().replace(microsecond=0)
        diff = {ATTR_WORD[attr]: (num * FRAME_COUNT)}
        start = end - timedelta(**diff)

        return (start, end)

    def generate_cache(self):
        q = self.db.query(
            Market.id,
            Market.name,
            func.min(Trade.created).label('first_trade'),
            func.max(Trade.created).label('last_trade')
        ).join(Trade).\
        group_by(Market.id)

        for market in q.all():
            self.generate_json(market)

    def generate_json(self, m):
        if not os.path.exists(OUT_DIR):
            os.mkdir(OUT_DIR)

        print("Market %s %s -> %s" % (m.name, m.first_trade, m.last_trade))
        for interval in INTERVAL_AGGREGATE.keys():
            (frame, fmt) = INTERVAL_AGGREGATE[interval]
            ranspans = arrow.Arrow.span_range(
                frame, m.first_trade, m.last_trade
            )

            for rs in ranspans:
                out = ''
                if isinstance(fmt, types.FunctionType):
                    out = fmt(rs[0])
                else:
                    out = rs[0].strftime(fmt)
                rel_path = out + '.json'
                to_path = OUT_DIR / m.name.lower() / interval / rel_path
                to_file = os.path.basename(to_path)
                to_dir = os.path.dirname(to_path)

                # If file already exists, skip
                if os.path.exists(to_path):
                    continue

                if not os.path.exists(to_dir):
                    os.makedirs(to_dir)

                print("%s %-3s %s %-17s" % (m.name.lower(), interval,
                    rs[0], rel_path), end='')

                r = self.get(m.id, interval, rs[0], rs[1])
                with open(to_path, 'w') as f:
                    f.write(json.dumps(r, sort_keys=True, indent=4))
                
                rows = len(r)
                size = os.path.getsize(to_path)
                print("%s rows, %s" % (
                    humanize.intword(rows),
                    humanize.naturalsize(size)
                ))

        print()

    def get(self, market_id, interval, start = None, end = None):

        if (not start or not end):
            (start, end) = self.get_date_range(interval)

        sql = SQL['ohlc']

        dt_func = {
            '1m': """
                CAST(strftime('%Y-%m-%d %H:%M', {value}) AS TEXT) || ':00'
            """,
            '5m': """
                CAST(strftime('%Y-%m-%d %H:', {value}) AS TEXT) ||
                CAST(printf('%02d', (
                CAST(strftime('%M',{value}) AS INT) / 5) * 5) AS TEXT) || ':00'
            """,
            '15m': """
                CAST(strftime('%Y-%m-%d %H:', {value}) AS TEXT) ||
                CAST(printf('%02d', (
                CAST(strftime('%M',{value}) AS INT) / 15) * 15) AS TEXT) || ':00'
            """,
            '1h': """
                CAST(strftime('%Y-%m-%d %H', {value}) AS TEXT) || ':00:00'
            """,
            '6h': """
                CAST(strftime('%Y-%m-%d ', {value}) AS TEXT) ||
                CAST(printf('%02d', (
                CAST(strftime('%H',{value}) AS INT) / 6) * 6) AS TEXT) || ':00:00'
            """,
            '1d': """
                date({value}) || ' 00:00:00'
            """
        }
        
        (num, attr) = INTERVAL_PARTS[interval]
        ATTR_WORD[attr]

        sqlparts = {
            'start': dt_func[interval].format(value="datetime('"+str(start)+"')"),
            'end': dt_func[interval].format(value="datetime('"+str(end)+"')"),
            #'start': "datetime('{}')".format(start),
            #'end': "datetime('{}')".format(end),
            'step': "+{} {}".format(num, ATTR_WORD[attr]),
            'interval': dt_func[interval].format(value='created')
        }
        sql = sql.format(**sqlparts)

        conn = self.db.connection()
        q = conn.execute(sql, (market_id,))

        result = []

        last_price = 0
        for row in q.fetchall():
            d = dict(row)
            # chart library doesn't support empty fields yet
            # https://github.com/tradingview/lightweight-charts/pull/294
            #if d.get('open') == 0:
            #    for k in ('open','high','low','close','value','volume'):
            #        d.pop(k)
            if d['close'] == 0:
                for k in ('open','high','low','close'):
                    d[k] = last_price

            last_price = d['close']
            result.append(d)
        
        # Ugly hack to account for issue 294 above
        result.reverse()
        last_price = 0
        for i, d in enumerate(result):
            if d['open'] == 0:
                for k in ('open','high','low','close'):
                    result[i][k] = last_price

            last_price = d['open']

        result.reverse()

        return result


