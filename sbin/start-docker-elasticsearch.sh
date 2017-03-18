#!/bin/bash

cd `dirname "$0"`;
cd ..

export APP_HOME=`pwd`;

echo $APP_HOME
echo

docker pull docker.elastic.co/elasticsearch/elasticsearch:5.2.2
mkdir -p /tmp/docker/elasticsearch
cp $APP_HOME/sbin/docker-compose.yml /tmp/docker/elasticsearch
cd /tmp/docker/elasticsearch
docker-compose up