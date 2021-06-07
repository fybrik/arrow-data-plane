#!/bin/sh

cd build/classes/java/main
java -cp "../../../libs/Java-FlightServer-1.0-SNAPSHOT-all.jar:." ibm.com.example.ExampleFlightServer > /dev/null &
