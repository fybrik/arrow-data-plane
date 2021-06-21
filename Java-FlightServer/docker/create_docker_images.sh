#!/bin/sh

if [ ! -f classes.tar.gz ]
  then echo creating tgz file
  cd ../build/classes/java/main/
  tar cvfz ../../../../docker/classes.tar.gz org/
  cd -
fi

if [ ! -f logging.xml ]
  then cp ../logging.xml .
fi

if [ ! -f Java-FlightServer-1.0-SNAPSHOT-all.jar ]
  then cp ../build/libs/Java-FlightServer-1.0-SNAPSHOT-all.jar .
fi


docker build --tag example_server:latest .
