#!/bin/sh

if [ $# -ne 2 ]; then
   echo "Usage: $0 <host> <port>"
   exit 1
fi

java --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED -Dlogback.configurationFile=file:../resources/logging.xml -cp "./build/libs/flight-client-all.jar" org.m4d.adp.flight_client.MyFlightClient $1 $2
