#!/bin/sh

if [ $# -ne 2 ]; then
   echo "Usage: $0 <host> <port>"
   exit 1
fi

cd build/classes/java/main
java --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED -Dlogback.configurationFile=file:../../../../logging.xml -cp "../../../libs/Java-FlightServer-1.0-SNAPSHOT-all.jar:." ibm.com.example.MyFlightClient $1 $2