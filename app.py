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
    price = fields.Int()
    amount = fields.Int()

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

@app.route("/api/accounts/", methods=["POST"])
def new_account():
    json_data = request.get_json()
    if not json_data:
        return {"message": "No input data provided"}, 400
    # Validate and deserialize input
    try:
        data = account_schema.load(json_data)
    except ValidationError as err:
        return err.messages, 422
    account = Account(
        name=data["name"]
    )
    db.session.add(account)
    db.session.commit()
    new_account = db.session.query(Account).get(account.id)
    result = account_schema.dump(new_account)
    return {"message": "Created new quote.", "account": result}

ENTITY = {
    'account': Account,
    'asset'  : Asset,
    'market' : Market
}

ENTITY_SCHEMA = {
    'account': AccountSchema,
    'asset'  : AssetSchema,
    'market' : MarketSchema
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


if __name__ == '__main__':
    app.run(debug=True)

