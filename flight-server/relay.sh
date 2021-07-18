#!/bin/sh

if [ $# -ne 1 ]; then
   echo "Usage: $0 <transform=true/false>"
   exit 1
fi


java --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED -Dlogback.configurationFile=file:../resources/logging.xml -cp "./build/libs/flight-server-all.jar" org.m4d.adp.flight_server.ExampleFlightServer -s relay -t $1 -h 0.0.0.0 -p 12232 -rh localhost -rp 12233
