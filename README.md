# Stroom Stats

[![Build Status](https://travis-ci.org/gchq/stroom-stats.svg?branch=master)](https://travis-ci.org/gchq/stroom-stats)

Stroom-Stats is currently work in progress and not in a usable state. More details will be provided when it is in a working state.

## Building

```bash
#runs all unit and integration tests - requires docker containers to be running (see below)
./gradlew clean downloadUrlDependencies build shadowJar

#runs only unit tests, no docker containers required
./gradlew clean downloadUrlDependencies build -x integrationTest

#Run a full build producing fat jars with a specified version number
./gradlew -Pversion=v1.2.3 clean downloadUrlDependencies build -x test -x integrationTest shadowJar
```

## Running
The `run` task in `build.gradle` looks for `config_dev.yml`. Requires dockers containers to be running (see below)

## Required Docker containers

To run integration tests or to run stroom-stats you will need docker containers for stroom, mysql (x2), zookeeper, hbase and kafka.

These can be started in another shell as follows:

```bash
cd ..
git clone https://github.com/gchq/stroom-resources.git
cd stroom-resources/bin
./bouncceIt.sh hbase-kafka-mysql-stroom-zk.yml
```
