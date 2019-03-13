#!/bin/bash

set -e

#run a build with no tests for speed
./buildNoIntTests.sh

#define the docker context as stroom-app/docker as this is where all the docker artefacts are, including the dockerfile
docker build \
  --tag=gchq/stroom-stats:v1.0.0-LOCAL-SNAPSHOT \
  --build-arg http_proxy=$http_proxy \
  --build-arg https_proxy=$https_proxy \
  ./docker/stroom-stats

docker build \
  --tag=gchq/stroom-stats-hbase:v1.0.0-LOCAL-SNAPSHOT \
  --build-arg http_proxy=$http_proxy \
  --build-arg https_proxy=$https_proxy \
  ./docker/stroom-stats-hbase
