#!/bin/sh
BIN="kafka_exporter"
case $1 in
    -darwin)
        OS="darwin"
        BUILD_ENV="GOOS=darwin GOARCH=amd64 CGO_ENABLED=1"
        LINUX_OPT="-race -gcflags=-N -gcflags=-l"
        ;;
    -win)
        OS="win"
        BUILD_ENV="GOOS=windows GOARCH=amd64 CGO_ENABLED=0"
        LINUX_OPT="-installsuffix cgo"
        ;;
    *)
        OS="linux"
        BUILD_ENV="GOOS=linux GOARCH=amd64 CGO_ENABLED=0"
        LINUX_OPT="-installsuffix cgo -ldflags=-s -ldflags=-w"
        ;;
esac

echo "start building..."

env $BUILD_ENV go build -v -o "$BIN.$OS" $LINUX_OPT

echo "done"
