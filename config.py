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


# Init dirs
for d in ALL_DIRS:
    if not os.path.exists(d):
        os.makedirs(d)

# Load SQL
for filename in glob.glob(str(SQL_DIR / '*.sql')):
    name = splitext(basename(filename))[0]
    with open(filename) as f:
      SQL[name] = f.read()

def entity_dict(db, model):
    entity = {}
    for table in db.engine.table_names():
        entity[table] = model.get_model_by_name(table)
    return entity

