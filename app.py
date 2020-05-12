#!/usr/bin/env python

import re
import json

from sqlalchemy import create_engine, and_, or_, dialects, func, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload, aliased

from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy

from marshmallow import Schema, fields, ValidationError, pre_load

from model import (
    Owner, Account, Asset, Market, Event, Order, Trade, Transaction
)

from libs import SQL

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///me.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class OwnerSchema(Schema):
    id = fields.Int(dump_only=True)
    name = fields.Str()
    email = fields.Str()
    title = fields.Str()

class AccountSchema(Schema):
    id = fields.Int(dump_only=True)
    name = fields.Str()
    owner = fields.Int()
    asset = fields.Int()
    balance = fields.Int()

class AssetSchema(Schema):
    id = fields.Int(dump_only=True)
    name = fields.Str()
    symbol = fields.Str()
    icon = fields.Str()

class MarketSchema(Schema):
    id = fields.Int(dump_only=True)
    name = fields.Str()

class EventSchema(Schema):
    id = fields.Int(dump_only=True)
    owner = fields.Int()
    action = fields.Str()
    payload = fields.Str()

class OrderSchema(Schema):
    id = fields.Int(dump_only=True)
    owner = fields.Int(required=True)

    #market = fields.Int(required=True)
    market = fields.Nested("MarketSchema", only=("id", "name"))

    direction = fields.Str(required=True)
    type = fields.Str()
    price = fields.Int(required=True)
    amount = fields.Int(required=True)
    amount_left = fields.Int(required=True)
    balance = fields.Int()
    status = fields.Str(dump_only=True)
    created = fields.DateTime(dump_only=True)

class TradeSchema(Schema):
    id = fields.Int(dump_only=True)
    #market = fields.Int()
    amount = fields.Int()
    price = fields.Int()
    created = fields.DateTime(dump_only=True)

class TransactionSchema(Schema):
    id = fields.Int(dump_only=True)
    name = fields.Str()
    account1 = fields.Int()
    account2 = fields.Int()
    price = fields.Int()
    amount = fields.Int()


@app.route('/')
def index():
    return 'Mock Exchange'

ENTITY = {
    'owner'  : Owner,
    'account': Account,
    'asset'  : Asset,
    'market' : Market,
    'event'  : Event,
    'order'  : Order,
    'trade'  : Trade
}

ENTITY_SCHEMA = {
    'owner'  : OwnerSchema,
    'account': AccountSchema,
    'asset'  : AssetSchema,
    'market' : MarketSchema,
    'event'  : EventSchema,
    'order'  : OrderSchema,
    'trade'  : TradeSchema
}

@app.route('/api/ohlc', methods=["GET"])
def get_ohlc():

    market = request.args.get('market')
    if not market:
        return {"message": "Market parameter required"}, 400

    sql = SQL['ohlc']
    q = db.engine.execute(sql, (request.args['market'],))

    result = []
    for row in q.fetchall():
        result.append(dict(row))

    return jsonify(result)


@app.route('/api/book', methods=["GET"])
def get_book():

    market = request.args.get('market')
    if not market:
        return {"message": "Market parameter required"}, 400

    sql = SQL['book']
    rs = db.engine.execute(sql, (market, market,))

    result = []
    for row in rs:
        result.append(dict(row))

    return jsonify(result)



@app.route('/api/<string:entity>', methods=["GET"])
@app.route('/api/<string:entity>/<int:pk>', methods=["GET"])
def get_entity(entity, pk=None):
    if entity not in ENTITY.keys():
        return {"message": "No such entity"}, 400

    Entity = ENTITY[entity]
    EntitySchema = ENTITY_SCHEMA[entity]

    result = None

    #print('args:',list(request.args.keys()))

    if pk:
        row = db.session.query(Entity).get(pk)
        result = EntitySchema().dump(row)
    else:
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
            elif oper == 'like':
                args.append((col.like(val)))
            else:
                args.append((col==val))
    
        q = db.session.query(Entity)
        q = q.filter(*args).order_by(*order)
        rows = q.paginate(page=page, per_page=per_page).items
        #rows = q.all()
        result = EntitySchema(many=True).dump(rows)

        blob = {
            'a': request.args,
            'results': result
        }

    return jsonify(result)

@app.route("/api/<string:entity>", methods=["POST"])
def new_entity(entity):
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

