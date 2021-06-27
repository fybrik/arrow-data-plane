#!/bin/sh

if [ ! -f logging.xml ]
  then cp ../logging.xml .
fi

if [ ! -f Java-FlightServer-1.0-SNAPSHOT-all.jar ]
  then cp ../build/libs/Java-FlightServer-1.0-SNAPSHOT-all.jar .
fi


docker build --tag example_server:latest .
