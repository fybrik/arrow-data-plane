#!/bin/sh

if [ ! -f logging.xml ]
  then cp ../resources/logging.xml .
fi

# if [ ! -f flight-server-all.jar ]
#   then cp ../flight-server/build/libs/flight-server-all.jar .
# fi
cp ../flight-server/build/libs/flight-server-all.jar .
# if [ ! -f flight-client-all.jar ]
#   then cp ../flight-client/build/libs/flight-client-all.jar .
# fi
cp ../flight-client/build/libs/flight-client-all.jar .

# docker build -f Dockerfile.client --tag example_client:latest .
# docker build -f Dockerfile.memory-server --tag example_memory:latest .
# docker build -f Dockerfile.relay-server --tag example_relay:latest .
# docker build -f Dockerfile.both-servers --tag example_both:latest .
docker build -f Dockerfile.relay-server2 --tag ghcr.io/mohammad-nassar10/relay-flight-server:latest .