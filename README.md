# Mock Exchange

Order matching system to investigate information flows.

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

TODO

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

