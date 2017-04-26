#!/bin/bash

function buildIt {
    if [ -f "build.gradle" ]; then
        ./gradlew clean build publishToMavenLocal -x test
    else
        mvn clean install
    fi
}

function sortItAllOut {
    if [[ -d $1 ]]; then
        # Update the repo
        echo -e "Checking repo \e[96m$1\e[0m for updates"
        cd $1
        git pull

        buildIt

        cd ..
    else
        # Clone the repo
        echo -e "Cloning \e[96m$1\e[0m"
        git clone https://github.com/gchq/$1.git
        cd $1

        buildIt

        cd ..
    fi
}

sortItAllOut stroom

# Set up the database - we need to set up a custom user otherwise we'll have trouble connecting in Travis CI
#mysql -e "CREATE DATABASE IF NOT EXISTS stroom;"
#mysql -e "CREATE USER 'stroomuser'@'localhost' IDENTIFIED BY 'stroompassword1';"
#mysql -e "GRANT ALL PRIVILEGES ON * . * TO 'stroomuser'@'localhost';"
#mysql -e "FLUSH PRIVILEGES"

# Install the urlDependencies plugin
git clone https://github.com/gchq/urlDependencies-plugin.git
cd urlDependencies-plugin
./gradlew clean build publishToMavenLocal