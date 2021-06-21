#!/bin/sh

kind load docker-image example_server:latest
kubectl run example-server --image=example_server:latest --image-pull-policy='Never'
