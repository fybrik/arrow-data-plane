#!/bin/bash

if [ $# -ne 2 ]; then
   echo "Usage: $0 [hostname] [port]"
   exit 1
fi

cd /root

time java --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED -Dlogback.configurationFile=file:logging.xml -cp "flight-client-all.jar" org.m4d.adp.flight_client.MyFlightClient $1 $2
