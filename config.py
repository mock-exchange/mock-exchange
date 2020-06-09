import glob
import os
from os.path import basename, splitext
from pathlib import Path

BASE_DIR = Path('.')

DATA_DIR  = BASE_DIR / 'data'  # seed
CACHE_DIR = BASE_DIR / 'cache'
SQL_DIR   = BASE_DIR / 'sql'

DT_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

DIRS = (DATA_DIR, CACHE_DIR, SQL_DIR)

for d in DIRS:
    if not os.path.exists(d):
        os.makedirs(d)


SQL = {}
for filename in glob.glob(str(SQL_DIR / '*.sql')):
    name = splitext(basename(filename))[0]
    with open(filename) as f:
      SQL[name] = f.read()


