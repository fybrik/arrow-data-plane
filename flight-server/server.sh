#!/bin/sh

java --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED -Dlogback.configurationFile=file:../resources/logging.xml -cp "./build/libs/flight-server-all.jar" org.m4d.adp.flight_server.ExampleFlightServer example localhost 12233
