#!/bin/sh

kind load docker-image example_server:latest
kubectl apply -f deploy.yaml
