#!/bin/bash
./gradlew clean build xjc -x integrationTest shadowJar "$@"
