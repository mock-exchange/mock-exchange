import os
from pathlib import Path
import json
import humanize
import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import math
import types
import time
from datetime_truncate import truncate
from itertools import groupby
import glob
import dateutil.parser
from collections import OrderedDict

from sqlalchemy import create_engine, and_, or_, func
from sqlalchemy.orm import Session, joinedload

from model import (Account, Market, Asset, Event, Order, Trade, Ledger)
from lib import SQL

OUT_DIR = Path('data/ohlc')
OUT24_DIR = Path('data/last24')

dt_format = '%Y-%m-%dT%H:%M:%SZ'

INTERVALS = ('5m','15m','1h','6h','1d')

ATTR_WORD = {
    'M': 'months',
    'w': 'weeks',
    'd': 'days',
    'h': 'hours',
    'm': 'minutes'
}

INTERVAL_PARTS = {}
for i in INTERVALS:
    attrs = ATTR_WORD.keys()
    (num, attr) = re.match("(\d+)([\w])",i).groups()
    INTERVAL_PARTS[i] = [
        int(num),
        attr
    ]

FRAME_COUNT = 500

"""
 1d/YYYY/YYYY.json                    year    (365 rows)
 6h/YYYY/QQ/YYYY-QQ.json              quarter (360 rows)
 1h/YYYY/MM/YYYY-MM.json              month   (720 rows)
15m/YYYY/WW/YYYY-WW.json              week    (672 rows)
 5m/YYYY/MM/DD/YYYY-MM-DD.json        day     (280 rows)
 1m/YYYY/MM/DD/HH/YYYY-MM-DDTHH.json  hour (60 rows or 1440 daily)
"""

INTERVAL_AGGREGATE = {
    '1d'  : ('year',    '%Y'),
    '6h'  : ('quarter', lambda dt: dt.strftime('%Y/Q') + str(math.ceil(dt.month/3))),
    '1h'  : ('month',   '%Y/%m'),
    '15m' : ('week',    '%Y/W%W'),
    '5m'  : ('day',     '%Y/%m/%d'),
    '1m'  : ('hour',    '%Y/%m/%d/%H'),
}

# Caching resolutions higher than 1h lose tz support. I guess this is
# why trading charts display a countdown to a common(UTC) close.
INTERVAL_FORMAT = {
    '1mo' : '%Y-%m-00 00:00:00',
    '1w'  : '%Y-%m-%d 00:00:00', # **
    '3d'  : '%Y-%m-%d 00:00:00',
    '1d'  : '%Y-%m-%d 00:00:00',
    '6h'  : '%Y-%m-%d %H:00:00', # **
    '1h'  : '%Y-%m-%d %H:00:00',
    '15m' : '%Y-%m-%d %H:%M:00', # **
    '5m'  : '%Y-%m-%d %H:%M:00', # **
    #'1m'  : '%Y-%m-%d %H:%M:00',
}

TRUNCATE = {
    '1d': 'day',
    '6h': '6_hour',
    '1h': 'hour',
    '15m': '15_minute',
    '5m': '5_minute'
}

JSONL_KEYS = ('dt', 'time', 'open', 'high', 'low', 'close', 'volume', 'value')

class OHLC:
    def __init__(self, session):
        self.db = session

        self.now = datetime.utcnow()
        #self.now = datetime(2020,6,1,2,2)
        #print("NOW:",self.now, self.now.tzinfo)

    def _aggfmt(self, dt, interval):
        (frame, fmt) = INTERVAL_AGGREGATE[interval]
        out = ''
        if isinstance(fmt, types.FunctionType):
            out = fmt(dt)
        else:
            out = dt.strftime(fmt)
        return out + '.jsonl'

    def _aggfile(self, m, dt, interval):
        return OUT_DIR / self._aggrelfile(m, dt, interval)

    def _aggrelfile(self, m, dt, interval):
        return Path(m.name.lower()) / interval / self._aggfmt(dt, interval)

    def get_range(self, interval, start, end):
        (num, attr) = INTERVAL_PARTS[interval]
        dt = truncate(start, TRUNCATE[interval])
        items = []

        while dt < end:
            items.append(dt)
            diff = {ATTR_WORD[attr]: num}
            dt = dt + timedelta(**diff)

        return items

    def get_span_range(self, interval, start, end):
        (frame, fmt) = INTERVAL_AGGREGATE[interval]
        dt = truncate(start, frame)
        spans = []

        while dt < end:
            if frame == 'quarter':
                diff = {'months': 3}
            else:
                diff = {frame + 's': 1}

            spans.append((
                dt, (dt + relativedelta(**diff)) - relativedelta(seconds=1)
            ))

            dt = dt + relativedelta(**diff)

        return spans

    def get_date_range(self, interval):

        (num, attr) = INTERVAL_PARTS[interval]
        end = datetime.utcnow().replace(microsecond=0)
        diff = {ATTR_WORD[attr]: (num * FRAME_COUNT)}
        start = end - timedelta(**diff)

        return (start, end)

    def generate_cache(self, market=None, stream=False):
        q = self.db.query(
            Market.id,
            Market.name,
            func.min(Trade.created).label('first_trade'),
            func.max(Trade.created).label('last_trade')
        ).join(Trade)

        if market:
            q = q.filter(Market.name == market)

        q = q.group_by(Market.id)

        for market in q.all():
            if stream:
                self.append_json(market)
            else:
                self.generate_json(market)

    def append_json(self, m):
        state_keys = ('open','high','low','close','volume')
        state_file = OUT_DIR / m.name.lower() / '.state.json'

        print("Processing",m.name)

        # 1. Get previous state
        state = {}
        if os.path.exists(state_file):
            f = open(state_file)
            state = json.loads(f.read())
            f.close()

        last_lines = {}
        last_row = {}
        prev_lines = {}
        for i in INTERVALS:
            print(i)
            my_path = str(OUT_DIR / m.name.lower() / i / '**/*.jsonl')
            files = sorted(glob.glob(my_path, recursive=True))

            if not len(files):
                raise ValueError(
                    'Files missing in {} for {}.'.format(i, m.name)
                )

            with open(files[-1]) as f:
                last_lines[i] = f.read().splitlines()

            # Used by last_24, only needs 1h
            if i == '1h' and len(files) >= 2:
                with open(files[-2]) as f:
                    prev_lines[i] = f.read().splitlines()

            last_row[i] = json.loads(last_lines[i][-1])

        # 1a. Validate latest state (all levels should be a subset)
        smallest_period = json.loads(last_lines[INTERVALS[0]][-1])
        smallest_dt = dateutil.parser.parse(smallest_period['dt'],
            ignoretz=True)
        for i in INTERVALS:
            this_period = json.loads(last_lines[i][-1])
            this_dt = dateutil.parser.parse(this_period['dt'], ignoretz=True)
            compare_dt = truncate(smallest_dt, TRUNCATE[i])
            if compare_dt != this_dt:
                raise ValueError(
                    "Last row {} doesn't match in {} for {}".format(
                        this_dt, i, m.name)
                )


        # 2. Get next batch of trades
        q = self.db.query(Trade).filter(Trade.market_id == m.id)
        q = q.filter(Trade.created <= self.now)
        if 'last_trade_id' in state:
            q = q.filter(Trade.id > state['last_trade_id'])
        else:
            q = q.filter(Trade.created >= smallest_dt)
        q = q.order_by(Trade.id.asc()).limit(1000)
        trades = q.all()


        # 3. Compute ohlcv updates
        updates = {}
        for i in INTERVALS:
            updates[i] = {}
            for key, group in groupby(trades, key=lambda x: truncate(x.created, TRUNCATE[i])):
                groups = [(i.price, i.amount, i.id) for i in list(group)]
                (prices, amounts, ids) = list(zip(*groups))

                data = {
                    'dt'     : key,
                    'open'   : prices[0],
                    'high'   : max(prices),
                    'low'    : min(prices),
                    'close'  : prices[-1],
                    'volume' : sum(amounts),
                    'last_trade_id' : ids[-1]
                }
                updates[i][key] = data


        # If there are no trades we need to create empty periods
        # between the last row and current period.
        if len(trades) == 0:
            start = smallest_dt
            end = self.now
        else:
            start = trades[0].created
            end = trades[-1].created

        print(start, '->', end)
        print("trades count:",len(trades))
        all_updates = OrderedDict()
        for i in INTERVALS:
            if i not in all_updates:
                all_updates[i] = OrderedDict()
            for dt in self.get_range(i, start, end):
                if dt in updates[i]:
                    all_updates[i][dt] = updates[i][dt]
                else:
                    data = {x:0 for x in state_keys}
                    data['dt'] = dt
                    all_updates[i][dt] = data

        # 4. Apply updates to last interval
        print()
        print("Apply updates..")
        out = OrderedDict()
        out_rows = {}
        for i in INTERVALS:
            out[i] = OrderedDict()
            out_rows[i] = {}
            for j, udt in enumerate(all_updates[i].keys()):
                u = all_updates[i][udt]
                rel_path = self._aggfmt(u['dt'], i)

                if rel_path not in out[i]:
                    out[i][rel_path] = []
                    out_rows[i][rel_path] = 0

                if j == 0:
                    p = last_lines[i].pop()
                    out[i][rel_path] = last_lines[i]

                    period = u['dt'].strftime(dt_format)
                    if period != last_row[i]['dt']:
                        raise ValueError(
                            "Last row {} doesn't match in {} for {}".format(
                                period, i, m.name)
                        )

                    # If we queried from last_trade_id update last_row, else take all
                    if 'last_trade_id' in state:
                        self._apply_prev_ohlcv(u, p)
                
                data = OrderedDict({k:u.get(k) for k in JSONL_KEYS})
                data['time'] = int(u['dt'].timestamp())
                data['dt'] = u['dt'].strftime(dt_format)
                for x in state_keys:
                    if u[x] == int(u[x]):
                        data[x] = int(u[x])
                    else:
                        data[x] = float(u[x])
                data['value'] = data['volume']

                out[i][rel_path].append(json.dumps(data))
                out_rows[i][rel_path] += 1
                print(i, rel_path, dict(data))

        moves = []

        print()
        print("Write updates:")
        for i in INTERVALS:
            for rel_path in out[i].keys():
                begin = time.time()

                to_path = OUT_DIR / m.name.lower() / i / rel_path
                to_tmp = str(to_path) + '.tmp'
                to_dir = os.path.dirname(to_path)

                print("%s %-3s %-25s" % (m.name.lower(), i,
                    rel_path + '.tmp'), end='')

                if not os.path.exists(to_dir):
                    os.makedirs(to_dir)

                f = open(to_tmp, "w")
                f.write("\n".join(out[i][rel_path]))
                f.flush()
                os.fsync(f.fileno())
                f.close()

                rows = out_rows[i][rel_path]
                size = os.path.getsize(to_tmp)
                print("%5d rows, %10s took %5.2fs" % (
                    rows,
                    humanize.naturalsize(size),
                    time.time() - begin
                ))

                moves.append((to_tmp, to_path))

        print()
        print("Commit updates:")
        for move in moves:
            print("rename to", str(move[1]))
            os.rename(move[0], move[1])

        print()
        print("Last 24")
        # Pull out last 24 stats from 1h rows
        cut = 24
        last24 = []
        for rel_path in reversed(list(out['1h'].keys())):
            tmp = out['1h'][rel_path][-cut:]
            cut = cut - len(tmp)
            last24.extend(reversed(tmp))
            if cut <= 0:
                break
        if cut > 0 and '1h' in prev_lines:
            tmp = prev_lines['1h'][-cut:]
            last24.extend(reversed(tmp))
        last24 = list(map(lambda x: json.loads(x), reversed(last24)))

        if len(last24):
            groups = [(i['low'], i['high'], i['volume']) for i in last24]
            (lows, highs, volumes) = list(zip(*groups))
            first = last24[0]
            last = last24[-1]
            data = {
                'market_id' : m.id,
                'name'      : m.name,
                'open'      : first['open'],
                'high'      : max(highs),
                'low'       : min(lows),
                'close'     : last['close'],
                'volume'    : sum(volumes),
                'change'    : (last['close'] - first['open']) / first['open'],
                # This is median; need to have avg included in ohlc
                'avg_price' : ((max(highs) - min(lows)) / 2) + min(lows)
            }
            print(data)

            if not os.path.exists(OUT24_DIR):
                os.makedirs(OUT24_DIR)

            l24_to_path = OUT24_DIR / str(str(m.id) + '.json')
            f = open(l24_to_path, "w")
            f.write(json.dumps(data))
            f.flush()
            os.fsync(f.fileno())
            f.close()


    def _apply_prev_ohlcv(self, u, p):
        # If there is a previous open, use it
        if p['open']:
            u['open'] = p['open']

        # If the previous high is higher, use it
        if p['high'] > u['high']:
            u['high'] = p['high']

        # If the previous low is lower, use it
        if p['low'] < u['low']:
            u['low'] = p['low']

        # Close is always the latest one

        # Sum this and previous volume
        u['volume'] += p['volume']


    def generate_json(self, m):
        if not os.path.exists(OUT_DIR):
            os.mkdir(OUT_DIR)

        start = m.first_trade
        end = self.now

        print("Market %s %s -> %s" % (m.name, start, end))
        print(start, type(start), start.tzinfo)
        print(end, type(end), end.tzinfo)
        for interval in INTERVALS:
            for sr in self.get_span_range(interval, start, end):
                rel_path = self._aggfmt(sr[0], interval)
                to_path = OUT_DIR / m.name.lower() / interval / rel_path
                to_tmp = str(to_path) + '.tmp'
                to_dir = os.path.dirname(to_path)

                if os.path.exists(to_path) and self.now > sr[1]:
                    continue

                if not os.path.exists(to_dir):
                    os.makedirs(to_dir)

                print("%s %-3s %-17s" % (m.name.lower(), interval,
                    rel_path), end='')

                begin = time.time()

                end_get = end if sr[1] > end else sr[1]
                r = self.get(m.id, interval, sr[0], end_get)

                lines = []
                for row in r:
                    lines.append(json.dumps(row))

                f = open(to_tmp, 'w')
                out = "\n".join(lines)
                f.write(out)
                f.flush()
                os.fsync(f.fileno())
                f.close()

                os.rename(to_tmp, to_path)

                rows = len(r)
                size = os.path.getsize(to_path)
                print("%5d rows, %10s took %5.2fs" % (
                    rows,
                    humanize.naturalsize(size),
                    time.time() - begin
                ))

        print()

    def get_last24_cached(self, market_id=None):
        if market_id:
            data = {}
            with open(OUT24_DIR / str(str(market_id) + '.json')) as f:
                data = json.loads(f.read())
            return data
        else:
            my_path = str(OUT24_DIR / '*.json')
            files = sorted(glob.glob(my_path))

            data = []
            for fn in files:
                with open(fn) as f:
                    data.append(json.loads(f.read()))

            return data

    def get_last24(self, market_id=None):

        sql = SQL['last24']

        result = []
        where = ''
        sub_where = ''
        values = []

        if market_id:
            where = 'AND m.id=?'
            sub_where = 'AND market_id=?'
            values.append((market_id, market_id))

        sql = sql.format(where=where, sub_where=sub_where)
        conn = self.db.connection()
        q = conn.engine.execute(sql, values)

        return dict(q.fetchone())

    def get_cached(self, market_id, interval, start=None, end=None):
        if (not start or not end):
            (start, end) = self.get_date_range(interval)

        m = self.db.query(Market).get(market_id)

        results = []
        print("start:",start)
        print("end  :",end)
        for sr in self.get_span_range(interval, start, end):
            rel_path = self._aggfmt(sr[0], interval)
            to_path = OUT_DIR / m.name.lower() / interval / rel_path
            print("to_path:",to_path)
            if os.path.exists(to_path):
                with open(to_path, 'r') as f:
                    page = [json.loads(x) for x in f.read().split("\n")]
                    results.extend(page)

        return results

    def get(self, market_id, interval, start=None, end=None):

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


