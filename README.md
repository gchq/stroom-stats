# Stroom Stats

[![Build Status](https://travis-ci.org/gchq/stroom-stats.svg?branch=master)](https://travis-ci.org/gchq/stroom-stats)

Stroom-Stats is currently work in progress and not in a usable state. More details will be provided when it is in a working state.

## Building

```bash
#runs all unit and integration tests - requires docker containers to be running
./gradlew clean build 

#runs only unit tests, no docker containers required
./gradlew clean build -x integrationTest
```

## Running
The `run` task in `build.gradle` looks for `config_dev.yml`.
