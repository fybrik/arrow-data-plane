#!/bin/sh

java --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED -Dlogback.configurationFile=file:../resources/logging.xml -cp "../build/libs/Java-FlightServer-1.0-SNAPSHOT-all.jar" org.m4d.adp.flight_server.ExampleFlightServer example
