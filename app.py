#!/usr/bin/env python
import sqlite3
from flask import Flask, request, g, jsonify
import json

from flask_sqlalchemy import SQLAlchemy
from marshmallow import Schema, fields, ValidationError, pre_load

from sqlalchemy.exc import IntegrityError
from sqlalchemy import create_engine, and_, or_, dialects, func, update
from sqlalchemy.orm import Session, joinedload

from model import Account

DATABASE = 'me.db'


app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///me.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

def session():
    session = getattr(g, '_session', None)
    if session is None:
        #db = g._database = sqlite3.connect(DATABASE)
        engine = create_engine('sqlite:///' + DATABASE)
        session = g._session = Session(engine)
    return session

@app.teardown_appcontext
def close_connection(exception):
    session = getattr(g, '_session', None)
    if session is not None:
        session.close()

"""
conn = sqlite3.connect('me.db')
conn.row_factory = model.dict_factory
db = conn.cursor()

engine = create_engine('sqlite:///me.db')
self.session = Session(engine)
"""

class AccountX(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80))


class AccountSchema(Schema):
    id = fields.Int(dump_only=True)
    name = fields.Str()


accounts_schema = AccountSchema(many=True)
account_schema = AccountSchema()

@app.route('/')
def index():
    return 'Mock Exchange'


@app.route('/api/accounts')
def get_accounts():
    accounts = session().query(Account).all()
    result = accounts_schema.dump(accounts)
    return {"accounts": result}

@app.route('/api/accounts/<int:pk>')
def get_account(pk):
    try:
        account = session().query(Account).get(pk)
    except IntegrityError:
        return {"message": "Account could no be found."}, 400
    result = account_schema.dump(account)
    return {"account": result}

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


@app.route('/api/accounts_xxx', methods=['GET'])
def get_accounts_xxx():
    where = []

    query = session().query(
      Account
    ).filter(
      and_(
        or_(*where)
      )
    ).order_by(
      Account.name
    )

    rows = []
    for r in query.all():
        d = dict(r.__dict__)
        d.pop('_sa_instance_state', None)
        rows.append(d)
        print(r)
        print(d)
    #print('-' *70)
    #print('rows:')
    #print(rows)
    #return json.dumps(rows)
    return jsonify(rows)



if __name__ == '__main__':
    app.run(debug=True)

