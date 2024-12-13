#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <tag>"
    exit 1
fi

TAG=$1

sudo docker build -t brunogb123/custom-k8s-scheduler:$TAG .
sudo docker push brunogb123/custom-k8s-scheduler:$TAG
