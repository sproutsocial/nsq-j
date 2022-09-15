#!/bin/sh
echo "Pruning containers"
docker rm -f $(docker ps --all --filter name=nsqd-cluster -q)  $(docker ps --all --filter name=nsq-lookup -q)
echo "Pruning networks"
docker network rm $(docker network ls -f name=nsq- -q)
echo "Done"
