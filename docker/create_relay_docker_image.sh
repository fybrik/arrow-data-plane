#!/bin/sh

cp ../resources/logging.xml .
cp ../flight-server/build/libs/flight-server-all.jar .
cp ../flight-client/build/libs/flight-client-all.jar .

docker build -f Dockerfile.relay-server --tag ghcr.io/fybrik/relay-flight-server:latest .