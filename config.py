import csv
import glob
import os
from os.path import basename, splitext
from pathlib import Path

BASE_DIR = Path('.')

DATA_DIR  = BASE_DIR / 'data'  # seed
CACHE_DIR = BASE_DIR / 'cache'
SQL_DIR   = BASE_DIR / 'sql'

DT_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
CSV_OPTS = { 'delimiter': ',', 'quotechar': '"', 'quoting': csv.QUOTE_MINIMAL }
SQL = {}

ALL_DIRS = (DATA_DIR, CACHE_DIR, SQL_DIR)

DB_CONN = 'postgres:///mockex'
RQ_CONN = 'redis://'

LOB_LMDB_NAME = 'orderbook'
LOB_LMDB_SIZE = (1024**2) * 400 # 400MB

# Init dirs
for d in ALL_DIRS:
    if not os.path.exists(d):
        os.makedirs(d)

# Load SQL
for filename in glob.glob(str(SQL_DIR / '*.sql')):
    name = splitext(basename(filename))[0]
    with open(filename) as f:
      SQL[name] = f.read()


