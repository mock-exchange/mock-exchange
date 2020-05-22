# Mock Exchange

Exchange order matching for acedemic purposes.

## Setup

```
$ git clone git@gitlab.com:eric-herrera/mock-exchange.git
$ virtualenv -p python3 venv
$ source venv/bin/activate
(venv) $ pip install -r requirements.txt
```

## Build

TODO

## Deploy

First deploy
```
$ alembic upgrade head
$ ./mockex import --entity all
$ ./mockex import [market] --ohlc=ohlc.csv # historical
$ ./mockex import [market] --trades=trades.csv
$ ./mockex [market] fill-order-book  # based on current market rate

```

## Database Migrations

If starting from an existed db without alembic, do the following. Then
manually update the version to start from.

First time, do:
```
$ alembic current
```

```
$ alembic upgrade head
```

Add new changes
```
$ alembic revision --autogenerate -m 'message'
```
