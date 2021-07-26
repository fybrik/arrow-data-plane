#!/bin/sh

kind load docker-image example_both:latest
kind load docker-image example_client:latest
kind load docker-image example_memory:latest
kind load docker-image example_relay:latest

kubectl apply -f both.yaml -f client.yaml -f memory.yaml -f relay.yaml
