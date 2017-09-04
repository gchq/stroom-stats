#!/bin/bash
./gradlew clean downloadUrlDependencies build -x integrationTest shadowJar "$@"
