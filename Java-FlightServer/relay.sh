#!/bin/sh

cd build/classes/java/main
java -Dlogback.configurationFile=file:../../../../logging.xml -cp "../../../libs/Java-FlightServer-1.0-SNAPSHOT-all.jar:." ibm.com.example.RelayFlightServer
