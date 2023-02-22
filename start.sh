#!/usr/bin/env bash
docker rm recipe-data-aggregator-app
docker rmi -f recipe-data-aggregator-image
docker build --no-cache -t recipe-data-aggregator-image .
CID=$(docker run -d -it -v $(pwd)/output:/output --name recipe-data-aggregator-app recipe-data-aggregator-image) ## mounting docker to host directory using -v attribute

docker logs -f $CID


