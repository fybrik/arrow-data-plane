#!/bin/sh

cd /root

java --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED -Dlogback.configurationFile=file:logging.xml -cp "Java-FlightServer-1.0-SNAPSHOT-all.jar" org.m4d.adp.flight_server.ExampleFlightServer example &
java --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED -Dlogback.configurationFile=file:logging.xml -cp "Java-FlightServer-1.0-SNAPSHOT-all.jar" org.m4d.adp.flight_server.ExampleFlightServer relay
