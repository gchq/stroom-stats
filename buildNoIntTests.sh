#!/bin/bash
./gradlew clean downloadUrlDependencies build xjc -x integrationTest shadowJar "$@"
