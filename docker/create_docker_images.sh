#!/bin/sh

if [ ! -f logging.xml ]
  then cp ../resources/logging.xml .
fi

if [ ! -f flight-server-all.jar ]
  then cp ../flight-server/build/libs/flight-server-all.jar .
fi

if [ ! -f flight-client-all.jar ]
  then cp ../flight-client/build/libs/flight-client-all.jar .
fi

docker build --tag example_server:latest .
