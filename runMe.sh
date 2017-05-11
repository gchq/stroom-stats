#!/bin/sh

checkContainerStatus() {
    if [ ! "$(docker ps -q -f name=$1 -f status=running)" ]; then
        echo "$1 docker container is not running"
        echo "Suggest running './bounceIt.sh hbase-kafka-mysql-stroom-zk.yml' in ../stroom-resources"
        exit 1
    fi
}

checkContainerStatus stroom-db
checkContainerStatus stroom-stats-db
checkContainerStatus zookeeper
checkContainerStatus kafka
checkContainerStatus hbase

#do the build without the integration tests for speed
./gradlew clean downloadUrlDependencies build -x integrationTest shadowJar

#run the app
find ./stroom-stats-service/build/libs -name "stroom-stats*-all.jar" -exec java -jar {} server ./stroom-stats-service/config_dev.yml \;
