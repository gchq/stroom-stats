#!/bin/sh

checkContainerStatus() {
    if [ ! "$(docker ps -q -f name=$1 -f status=running)" ]; then
        echo "$1 docker container is not running"
        echo "Suggest running './bounceIt.sh hbase-kafka-mysql-stroom-zk.yml' in ../stroom-resources"
        exit 1
    fi
}

#while stroom-stats does not depend on stroom, stroom will need to have been run to set up the database tables
#(in stroom-db) read by stroom-stats
checkContainerStatus stroom
checkContainerStatus stroom-db
checkContainerStatus zookeeper
checkContainerStatus kafka
checkContainerStatus hbase

#do the build without the integration tests for speed
./gradlew clean build -x integrationTest shadowJar

#run the app
find ./stroom-stats-service/build/libs -name "stroom-stats*-all.jar" -exec java -jar {} server ./stroom-stats-service/config_dev.yml \;
