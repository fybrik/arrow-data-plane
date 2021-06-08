#!/bin/sh

if [ $# -ne 1 ]; then
   echo Usage: $0 [direct / relay]
   exit 1
fi

cd build/classes/java/main
java -Dlogback.configurationFile=file:../../../../logging.xml -cp "../../../libs/Java-FlightServer-1.0-SNAPSHOT-all.jar:." ibm.com.example.RunBenchmark $1
