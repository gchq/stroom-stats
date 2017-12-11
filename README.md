# Stroom Stats

[![Build Status](https://travis-ci.org/gchq/stroom-stats.svg?branch=master)](https://travis-ci.org/gchq/stroom-stats)

Stroom-Stats is currently work in progress and not in a usable state. More details will be provided when it is in a working state.

## Building

```bash
#runs all unit and integration tests - requires docker containers to be running (see below)
./gradlew clean build shadowJar

#runs only unit tests, no docker containers required
./gradlew clean build -x integrationTest

#Run a full build producing fat jars with a specified version number
./gradlew -Pversion=v1.2.3 clean build -x test -x integrationTest shadowJar
```

## Running
The `run` task in `build.gradle` looks for `config_dev.yml`. Requires dockers containers to be running (see below)

### Health checks
The DropWizard health checks can be viewed at `http://localhost:8087/healthcheck?pretty=true` where 8087 is the admin port.

The health checks include all property values held in Zookeeper, states of all the statistic event processors and aggregators.

### Changing the logging level
The logging level can be changed at runtime by executing something similar to this (assuming you have HTTPie installed)

```bash
http -f POST http://localhost:8087/tasks/log-level logger=stroom.stats.StatisticsStore level=DEBUG
```

Where `8087` is the admin port.

Active logging levels can be viewed on the health check page [http://localhost:8087/healthcheck](http://localhost:8087/healthcheck) (where 8087 is the admin port) under the `stroom.stats.logging.LogLevelInspector` section.

### Stopping and starting processing
The processing of statistic events can be stopped and started via an admin task. By default all processing starts on application startup. To stop processing:

```bash
http -f POST http://localhost:8087/tasks/stopProcessing
```

To start processing:

```bash
http -f POST http://localhost:8087/tasks/startProcessing
```

(Where `8087` is the admin port)

## Required Docker containers

To run integration tests or to run stroom-stats you will need docker containers for stroom, mysql (x2), zookeeper, hbase and kafka.

These can be started in another shell as follows:

```bash
cd ..
git clone https://github.com/gchq/stroom-resources.git
cd stroom-resources/bin
./bouncceIt.sh hbase-kafka-mysql-stroom-zk.yml
```

## Licence

Stroom-Stats is licensed under [LGPL-v3](LICENSE.md), however the stroom-stats-schema module and associated published library is entirely auto-generated from an XML Schema hosted by [stroom-content](https://github.com/gchq/stroom-content). This XML Schema and the stroom-stats-schema library are licensed under the _Apache License, Version 2.0_.
