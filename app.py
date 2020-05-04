#!/usr/bin/env python

from sqlalchemy import create_engine, and_, or_, dialects, func, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload

from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy

from marshmallow import Schema, fields, ValidationError, pre_load

from model import (
    Account, Asset, Market, Order, Transaction
)

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///me.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class AccountSchema(Schema):
    id = fields.Int(dump_only=True)
    name = fields.Str()

class AssetSchema(Schema):
    id = fields.Int(dump_only=True)
    name = fields.Str()

class MarketSchema(Schema):
    id = fields.Int(dump_only=True)
    name = fields.Str()

class OrderSchema(Schema):
    id = fields.Int(dump_only=True)
    owner = fields.Int(required=True)
    market = fields.Int(required=True)
    direction = fields.Str(required=True)
    type = fields.Str()
    price = fields.Int(required=True)
    amount = fields.Int(required=True)
    amount_left = fields.Int(required=True)
    balance = fields.Int()
    status = fields.Str(dump_only=True)

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
    'account': Account,
    'asset'  : Asset,
    'market' : Market,
    'order'  : Order
}

ENTITY_SCHEMA = {
    'account': AccountSchema,
    'asset'  : AssetSchema,
    'market' : MarketSchema,
    'order'  : OrderSchema
}

@app.route('/api/<string:entity>', methods=["GET"])
@app.route('/api/<string:entity>/<int:pk>', methods=["GET"])
def get_entity(entity, pk=None):
    if entity not in ENTITY.keys():
        return {"message": "No such entity"}, 400

    Entity = ENTITY[entity]
    EntitySchema = ENTITY_SCHEMA[entity]

    result = None

    if pk:
        row = db.session.query(Entity).get(pk)
        result = EntitySchema().dump(row)
    else:
        rows = db.session.query(Entity).all()
        result = EntitySchema(many=True).dump(rows)

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

