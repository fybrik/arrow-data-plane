#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import pyarrow.flight as fl
import json

request = {
    "asset": "nyc-taxi.parquet", 
    "columns": ["vendor_id", "pickup_at", "dropoff_at", "payment_type"]
}

def main(port):
    client = fl.connect("grpc://localhost:{}".format(port))
    info = client.get_flight_info(
        fl.FlightDescriptor.for_command(json.dumps(request)))
    endpoint = info.endpoints[0]
    result: fl.FlightStreamReader = client.do_get(endpoint.ticket)
    i = 0
    for batch in result:
        i = i + 1
        print(i)
    print(batch.data.to_pandas())
    #print(result.read_pandas())


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='arrow-flight-module sample')
    parser.add_argument(
        '--port', type=int, default=12232, help='Listening port')
    args = parser.parse_args()

    main(args.port)
