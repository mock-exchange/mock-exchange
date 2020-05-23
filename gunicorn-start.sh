#!/bin/bash

NAME="mockex"
#DIR=/home/eric/apps/forklift # project dir
DIR=/home/eric/Work/mock-exchange/mock-exchange
USER=eric # run as
GROUP=eric # run as webapps
NUM_WORKERS=1

echo "Starting $NAME as `whoami`"

# Activate the virtual environment
cd $DIR
source venv/bin/activate

# Programs meant to be run under supervisor should not daemonize themselves (do not use --daemon)
exec gunicorn app:app \
  --name $NAME \
  --workers $NUM_WORKERS \
  --user=$USER \
  --bind 127.0.7.20:5000
# --log-level=debug \

