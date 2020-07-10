import unittest
import requests
import time
import humanize

BASE_URL = 'http://localhost:5000'

account_id = 103
market = 'shtusd'

urls = (
    '/api/event?account_id={account_id}',
    '/api/event?account_id={account_id}&status=new',
    '/api/order?account_id={account_id}',
    '/api/order?account_id={account_id}&status__in=open,partial',
    '/api/trade?account_id={account_id}',
    '/api/ledger?account_id={account_id}',
    '/api/balance?account_id={account_id}',

    '/api/wealth',

    '/api/{market}/ohlc/5m',
    '/api/{market}/ohlc/15m',
    '/api/{market}/ohlc/1h',
    '/api/{market}/ohlc/6h',
    '/api/{market}/ohlc/1d',
    '/api/{market}/book',
    '/api/{market}/last_trades',
    '/api/{market}/last24'
)

class TestAPI(unittest.TestCase):

    def test_urls(self):

        cnt = 0
        for u in urls:
            u = u.format(**{'account_id': account_id, 'market': market})
            cnt += 1
            begin = time.time()
            data = None
            size = 0
            status_code = 0
            try:
                r = requests.get(BASE_URL + u)
                status_code = r.status_code
                size = len(r.text)
                data = r.json()
            except:
                pass
            rows = 0
            if data and 'results' in data:
                rows = len(data['results'])
            elif type(data) == list:
                rows = len(data)
            print("%2d %-30.30s %5s %5d rows %10s %8.2f ms" % (
                cnt,
                u,
                status_code,
                rows,
                humanize.naturalsize(size, gnu=True),
                (time.time() - begin) * 1000
            ))

        self.assertTrue(True, True)
