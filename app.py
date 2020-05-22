#!/usr/bin/env python

import re
import json

from sqlalchemy import create_engine, and_, or_, dialects, func, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload, aliased

from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy

from marshmallow import Schema, fields, ValidationError, pre_load
from marshmallow import post_dump

import model

from mocklib import SQL

app = Flask(__name__, static_folder='foo')

#app.config['STATIC_FOLDER'] = 'foo'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///mockex.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)


class OwnerSchema(Schema):
    id = fields.Int(dump_only=True)
    name = fields.Str()
    email = fields.Str()
    username = fields.Str()
    picture = fields.Str()
    profile = fields.Str(dump_only=True)

    """
    @post_dump
    def make_json(self, data, **kwargs):
        print("make_json. data:",type(data), data)
        if 'profile' in data:
            data['profile'] = json.loads(data['profile'])
        return data
    """

class AssetSchema(Schema):
    id = fields.Int(dump_only=True)
    name = fields.Str()
    symbol = fields.Str()
    icon = fields.Str()

class MarketSchema(Schema):
    id = fields.Int(dump_only=True)
    name = fields.Str()
    asset = fields.Nested("AssetSchema")
    uoa = fields.Nested("AssetSchema")

class EventSchema(Schema):
    id = fields.Int(dump_only=True)
    status = fields.Str()
    method = fields.Str()
    owner_id = fields.Int()
    body = fields.Str()
    created = fields.Str(dump_only=True)
    uuid = fields.Str(dump_only=True)

class OrderSchema(Schema):
    id = fields.Int(dump_only=True)

    owner = fields.Nested("OwnerSchema", only=("id", "name"))
    market = fields.Nested("MarketSchema", only=("id", "name"))

    price = fields.Str(required=True)
    amount = fields.Str(required=True)
    balance = fields.Str(dump_only=True)

    event_uuid = fields.Str(dump_only=True)

    side = fields.Str(required=True)
    type = fields.Str(required=True)
    status = fields.Str(dump_only=True)
    created = fields.DateTime(dump_only=True)
    modified = fields.DateTime(dump_only=True)

class TradeSchema(Schema):
    id = fields.Int(dump_only=True)
    market = fields.Nested("MarketSchema", only=("id", "name"))
    #owner = fields.Nested("OwnerSchema", only=("id", "name"))
    order = fields.Nested("OrderSchema", only=("id", "status"))
    price = fields.Str(dump_only=True)
    amount = fields.Str(dump_only=True)
    created = fields.DateTime(dump_only=True)

class LedgerSchema(Schema):
    id = fields.Int(dump_only=True)
    #owner = fields.Nested("OwnerSchema", only=("id", "name"))
    order = fields.Nested("OrderSchema", only=("id", "status"))
    trade = fields.Nested("TradeSchema", only=("id",))
    price = fields.Str(dump_only=True)
    amount = fields.Str(dump_only=True)
    created = fields.DateTime(dump_only=True)


@app.route('/')
def index():
    return 'Mock Exchange'


"""
for table in model.Base.metadata.tables.keys():
    pass
"""

ENTITY = {}

for table in db.engine.table_names():
    ENTITY[table] = model.get_model_by_name(table)

"""
ENTITY = {
    'owner'  : model.Owner,
    'asset'  : model.Asset,
    'market' : model.Market,
    'event'  : model.Event,
    'order'  : model.Order,
    'trade'  : model.Trade,
    'ledger' : model.Ledger
}
"""

ENTITY_SCHEMA = {
    'owner'  : OwnerSchema,
    'asset'  : AssetSchema,
    'market' : MarketSchema,
    'event'  : EventSchema,
    'order'  : OrderSchema,
    'trade'  : TradeSchema,
    'ledger' : LedgerSchema
}

@app.route('/api/balance', methods=["GET"])
def get_balance():

    owner_id = request.args.get('owner_id')
    if not owner_id:
        return {"message": "owner_id parameter required"}, 400

    sql = SQL['balance']
    rs = db.engine.execute(sql, (owner_id,))

    result = []
    for row in rs:
        result.append(dict(row))

    return jsonify(result)


@app.route('/api/last24/<int:market_id>', methods=["GET"])
@app.route('/api/last24', methods=["GET"])
def get_last24(market_id = None):

    sql = SQL['last24']

    result = []
    where = ''
    values = []

    if market_id:
        where = 'AND market_id=?'
        values.append((market_id, market_id))

    sql = sql.format(where=where)
    rs = db.engine.execute(sql, values)

    if market_id:
        result = dict(rs.fetchone())
    else:
        for row in rs:
            result.append(dict(row))

    return jsonify(result)


@app.route('/api/ohlc', methods=["GET"])
def get_ohlc():

    market_id = request.args.get('market_id')
    interval = request.args.get('interval')

    missing = []
    if not market_id:
        missing.append('market_id')
    if not interval:
        missing.append('interval')
    if missing:
        params = ' and '.join(missing)
        return {"message": "parameter(s) " + params + " required"}, 400

    sql = SQL['ohlc']

    # Target 140 - 200 intervals total
    # 1m  - 140  - 200 minutes
    # 5m  - 700  - 1000 minutes
    # 15m - 2100 - 3000 minutes
    # 1h  - 140  - 200 hours
    # 6h  - 960  - 1200 hours
    # 1d  - 140  - 200 days
    intervals = {
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
    steps = {
        '1m' : '+1 minute',
        '5m' : '+5 minute',
        '15m': '+15 minutes',
        '1h' : '+1 hour',
        '6h' : '+6 hours',
        '1d' : '+1 day'
    }
    steps_int = {
        '1m' : 1,
        '5m' : 5,
        '15m': 15,
        '1h' : 1,
        '6h' : 6,
        '1d' : 1
    }
    steps_xx = {
        '1m' : 'minutes',
        '5m' : 'minutes',
        '15m': 'minutes',
        '1h' : 'hours',
        '6h' : 'hours',
        '1d' : 'days'
    }

    step_val = steps_int[interval] * 500
    
    start = "datetime('now', '-{} {}')".format(step_val, steps_xx[interval])
    sqlparts = {
        'start': intervals[interval].format(value=start),
        'end'  : intervals[interval].format(value="'now'"),
        'step': steps[interval],
        'interval': intervals[interval].format(value='created')
    }
    sql = sql.format(**sqlparts)

    print(sql)
    q = db.engine.execute(sql, (market_id,))

    result = []
    for row in q.fetchall():
        d = dict(row)
        # chart library doesn't support empty fields yet
        # https://github.com/tradingview/lightweight-charts/pull/294
        #if d.get('open') == 0:
        #    for k in ('open','high','low','close','value','volume'):
        #        d.pop(k)
        result.append(d)

    return jsonify(result)


@app.route('/api/book', methods=["GET"])
def get_book():

    market_id = request.args.get('market_id')
    if not market_id:
        return {"message": "market_id parameter required"}, 400

    sql = SQL['book']
    rs = db.engine.execute(sql, (market_id, market_id,))

    result = []
    for row in rs:
        result.append(dict(row))

    return jsonify(result)


@app.route('/api/<string:entity>/<int:pk>', methods=["GET"])
def get_entity_id(entity, pk):
    if entity not in ENTITY.keys():
        return {"message": "No such entity"}, 400

    Entity = ENTITY[entity]
    EntitySchema = ENTITY_SCHEMA[entity]

    result = None

    row = db.session.query(Entity).get(pk)
    result = EntitySchema().dump(row)

    return jsonify(result)


@app.route('/api/<string:entity>', methods=["GET"])
def get_entity_list(entity):
    if entity not in ENTITY.keys():
        return {"message": "No such entity"}, 400

    Entity = ENTITY[entity]
    EntitySchema = ENTITY_SCHEMA[entity]

    result = None

    order = []
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    args = []

    if 'order' in request.args:
        raw = request.args.get('order')
        ss = raw.split('.')
        k = ss[0]
        sortdir = None
        if len(ss) > 1:
            sortdir = ss[1]
        col = getattr(Entity, k)
        if sortdir == 'asc':
            order.append(col.asc())
        else:
            order.append(col.desc())

    valid = Entity.__table__.columns.keys()
    for raw in request.args:
        ss = raw.split('__')
        k = ss[0]
        oper = None
        if len(ss) > 1:
            oper = ss[1]

        if k not in valid:
            continue
        col = getattr(Entity, k)
        val = request.args[raw]
        if oper == 'in':
            vals = val.split(',')
            args.append((col.in_(vals)))
        elif oper == 'notin':
            vals = val.split(',')
            args.append(col.notin_(vals))
        elif oper == 'like':
            args.append((col.like(val)))
        else:
            args.append((col==val))

    q = db.session.query(Entity)
    q = q.filter(*args).order_by(*order)
    page = q.paginate(page=page, per_page=per_page)
    results = EntitySchema(many=True).dump(page.items)

    result = {
        'pagination': {
            'total': page.total,
            'page': page.page,
            'per_page': page.per_page,
            'has_next': page.has_next,
            'has_prev': page.has_prev
        },
        'results': results
    }

    return jsonify(result)

@app.route("/api/<string:entity>", methods=["POST"])
def create_entity(entity):
    if entity not in ENTITY.keys():
        return {"message": "No such entity"}, 400

    Entity = ENTITY[entity]
    EntitySchema = ENTITY_SCHEMA[entity]

    json_data = request.get_json()
    if not json_data:
        return {"message": "No input data provided"}, 400
    # Validate and deserialize input
    try:
        data = EntitySchema().load(json_data)
    except ValidationError as err:
        return err.messages, 422
    new_entity = Entity(
        **data
    )
    db.session.add(new_entity)
    db.session.commit()
    entity_out = db.session.query(Entity).get(new_entity.id)
    result = EntitySchema().dump(entity_out)
    return {"message": "Created new " + entity + ".", entity: result}


if __name__ == '__main__':
    app.run(debug=True)

