#!/bin/zsh

echo "start"
docker build -t kafka-runner:latest .
docker run -it --rm --name kafka-runner kafka-runner:latest

