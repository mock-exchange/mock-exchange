#!/bin/sh

NAME='mock-exchange'

# Do not run as root
if [ $(id -u) = 0 ]; then
   echo "This script must not be run as root"
   exit 1
fi


dev() {
    # Run dev setup
    rm -r ./venv
    virtualenv -p python3 ./venv
    . ./venv/bin/activate
    pip install -r ./requirements.txt
}

run() {
    # Run dev
    . ./venv/bin/activate
    export FLASK_APP=app.py
    export FLASK_DEBUG=1
    flask run
}

build() {
    # Run build
    TAG=`git describe`
    PKG_NAME=${NAME}-${TAG}
    echo Making ${PKG_NAME}
    git archive --format=tar --prefix=${PKG_NAME}/ ${TAG} > ${PKG_NAME}.tar
    
    tar --append --transform "s,^,${PKG_NAME}/," -f ${PKG_NAME}.tar build
    gzip -f ${PKG_NAME}.tar
}



case "$1" in
    dev)
        dev
        ;;

    run)
        run
        ;;

    build)
        build
        ;;
    *)
        echo "Usage: $_ {dev|run|build}"
        exit 1
        ;;
esac
