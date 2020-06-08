#!/usr/bin/env python

import re
import json
import time

from sqlalchemy import create_engine, and_, or_, dialects, func, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload, aliased

from flask import Flask, Blueprint, request, jsonify
from flask_sqlalchemy import SQLAlchemy

from marshmallow import Schema, fields, ValidationError, pre_load
from marshmallow import post_dump

import model
from lib import SQL, TradeFile
import ohlc
from ohlc import OHLC

#app = Flask(__name__, static_folder='foo')
app = Flask(__name__, static_folder='build', static_url_path='/')

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///mockex.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False


#app.config['STATIC_FOLDER'] = 'foo'
db = SQLAlchemy(app)


dt_format = '%Y-%m-%dT%H:%M:%SZ'

class AccountSchema(Schema):
    id = fields.Int(dump_only=True)
    uuid = fields.Str(dump_only=True)
    name = fields.Str()
    email = fields.Str()
    title = fields.Str()
    location = fields.Str()

class AssetSchema(Schema):
    id = fields.Int(dump_only=True)
    uuid = fields.Str(dump_only=True)
    name = fields.Str()
    symbol = fields.Str()
    icon = fields.Str()
    scale = fields.Int()

class MarketSchema(Schema):
    id = fields.Int(dump_only=True)
    uuid = fields.Str(dump_only=True)
    code = fields.Str()
    name = fields.Str()
    asset = fields.Nested("AssetSchema")
    uoa = fields.Nested("AssetSchema")

class EventSchema(Schema):
    id = fields.Int(dump_only=True)
    uuid = fields.Str(dump_only=True)
    status = fields.Str(dump_only=True)
    created = fields.DateTime(dump_only=True, format=dt_format)

    method = fields.Str(required=True)
    account_id = fields.Int(required=True)
    body = fields.Str(required=True)

class ValidateEventBody():
    def __init__(self):
        self.valid = {
            'place-order': (
                ('price', 1.1),
                ('amount',1.1),
                ('market_id',1),
                ('side', ('buy','sell')),
             ),
            'cancel-order': (
                ('uuid','22'),
            )
        }

    def load(self, data):
        try:
            body = json.loads(data['body'])
        except:
            raise ValidationError({'body': ['Not valid json.']})

        errors = {}
        rules = self.valid.get(data['method']) or []
        for rule in rules:
            (key, cond) = rule
            if key not in body:
                errors[key] = 'Missing data for required field.'
            elif type(cond) == tuple:
                if body[key] not in cond:
                    errors[key] = 'Not valid. Must be in %s.' % (','.join(cond))
            elif type(cond) == str and len(cond) and len(body[key]) != int(cond):
                errors[key] = 'Not valid length. Must be %d chars.' % int(cond)
            elif type(cond) == float:
                try:
                    body[key] = float(body[key])
                except:
                    errors[key] = 'Not valid float type.'
            elif type(body[key]) != type(cond):
                errors[key] = 'Not valid type. Must be %s.' % (cond.__class__.__name__)

        if errors:
            raise ValidationError({'body':errors})

        return body

class OrderSchema(Schema):
    id = fields.Int(dump_only=True)

    account = fields.Nested("AccountSchema", only=("id", "name"))
    market = fields.Nested("MarketSchema", only=("id", "name"))

    price = fields.Str(required=True)
    amount = fields.Str(required=True)
    balance = fields.Str(dump_only=True)

    uuid = fields.Str(dump_only=True)

    side = fields.Str(required=True)
    type = fields.Str(required=True)
    status = fields.Str(dump_only=True)
    created = fields.DateTime(dump_only=True, format=dt_format)
    modified = fields.DateTime(dump_only=True, format=dt_format)

class TradeSchema(Schema):
    id = fields.Int(dump_only=True)
    uuid = fields.Str(dump_only=True)

    market = fields.Nested("MarketSchema", only=("id", "name"))
    #account = fields.Nested("AccountSchema", only=("id", "name"))
    order = fields.Nested("OrderSchema", only=("id", "status"))
    price = fields.Str(dump_only=True)
    amount = fields.Str(dump_only=True)
    created = fields.DateTime(dump_only=True, format=dt_format)

class LedgerSchema(Schema):
    id = fields.Int(dump_only=True)
    uuid = fields.Str(dump_only=True)
    #account_id = fields.Int()
    account = fields.Nested("AccountSchema", only=("id", "name"))
    asset = fields.Nested("AssetSchema")
    order = fields.Nested("OrderSchema", only=("id", "status"))
    trade = fields.Nested("TradeSchema", only=("id",))
    price = fields.Str(dump_only=True)
    amount = fields.Str(dump_only=True)
    balance = fields.Str(dump_only=True)
    created = fields.DateTime(dump_only=True, format=dt_format)

"""
for table in model.Base.metadata.tables.keys():
    pass
"""

ENTITY = {}

for table in db.engine.table_names():
    ENTITY[table] = model.get_model_by_name(table)

"""
ENTITY = {
    'account'  : model.Account,
    'asset'  : model.Asset,
    'market' : model.Market,
    'event'  : model.Event,
    'order'  : model.Order,
    'trade'  : model.Trade,
    'ledger' : model.Ledger
}
"""

ENTITY_SCHEMA = {
    'account': AccountSchema,
    'asset'  : AssetSchema,
    'market' : MarketSchema,
    'event'  : EventSchema,
    'order'  : OrderSchema,
    'trade'  : TradeSchema,
    'ledger' : LedgerSchema
}


# No need to go to the db for this everytime.
MARKETS_CACHE = {}
def get_market(code):
    if not MARKETS_CACHE:
        for m in db.session.query(model.Market).all():
            MARKETS_CACHE[m.code] = m
        MARKETS_CACHE_TIME = time.time()
    return MARKETS_CACHE[code]

@app.route('/api/<string:market>/ohlc/<string:interval>', methods=["GET"])
def get_ohlc(market, interval):
    m = get_market(market)
    if not m:
        return {"message": "Invalid market"}, 400

    if interval not in ohlc.INTERVALS:
        return {"message": "Invalid interval"}, 400

    result = OHLC(db.session).get_cached(m, interval)
    return jsonify(result)

@app.route('/api/<string:market>/book', methods=["GET"])
def get_book(market):
    m = get_market(market)
    if not m:
        return {"message": "Invalid market"}, 400

    sql = SQL['book']
    rs = db.engine.execute(sql, {'market_id': m.id,})

    result = []
    for row in rs:
        result.append(dict(row))

    return jsonify(result)

@app.route('/api/<string:market>/last24', methods=["GET"])
def get_last24(market):
    m = None
    if market != 'all':
        m = get_market(market)

    if not m and market != 'all':
        return {"message": "Invalid market"}, 400

    result = OHLC(db.session).get_last24_cached(m)
    return jsonify(result)

@app.route('/api/<string:market>/last_trades', methods=["GET"])
def get_last_trades(market):
    m = get_market(market)
    if not m:
        return {"message": "Invalid market"}, 400

    result = TradeFile().get(m)
    return jsonify(result)


# Get account balance
@app.route('/api/balance', methods=["GET"])
def get_balance():

    account_id = request.args.get('account_id')
    if not account_id:
        return {"message": "account_id parameter required"}, 400

    sql = SQL['balance']
    rs = db.engine.execute(sql, {'account_id':account_id,})

    result = []
    for row in rs:
        result.append(dict(row))

    return jsonify(result)



# Get one
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


# Get list
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

# Create
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

    # Special validation
    if Entity == model.Event:
        try:
            body = ValidateEventBody().load(data)
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


#@app.route('/', defaults={'path': ''})
#@app.route('/<path>')
@app.route('/')
def index():
    #return 'You want path: %s' % path
    return app.send_static_file('index.html')

#@app.errorhandler(404)
#def not_found(e):
#    return app.send_static_file('index.html')


if __name__ == '__main__':
    app.run(debug=True)

