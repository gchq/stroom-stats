#!/bin/bash

#exit script on any error
set -e

DOCKER_REPO="gchq/stroom-stats"
GITHUB_REPO="gchq/stroom-stats"
DOCKER_CONTEXT_ROOT="docker/."
FLOATING_TAG=""
SPECIFIC_TAG=""
#This is a whitelist of branches to produce docker builds for
BRANCH_WHITELIST_REGEX='(^dev$|^master$|^v[0-9].*$)'
doDockerBuild=false

#Shell Colour constants for use in 'echo -e'
#e.g.  echo -e "My message ${GREEN}with just this text in green${NC}"
RED='\033[1;31m'
GREEN='\033[1;32m'
YELLOW='\033[1;33m'
BLUE='\033[1;34m'
NC='\033[0m' # No Colour 

createGitTag() {
    tagName=$1
    
    git config --global user.email "builds@travis-ci.com"
    git config --global user.name "Travis CI"

    echo -e "Tagging commit [${GREEN}${TRAVIS_COMMIT}${NC}] with tag [${GREEN}${tagName}${NC}]"
    git tag -a ${tagName} ${TRAVIS_COMMIT} -m "Automated Travis build $TRAVIS_BUILD_NUMBER" 
    #TAGPERM is a travis encrypted github token, see 'env' section in .travis.yml
    git push -q https://$TAGPERM@github.com/${GITHUB_REPO} ${tagName}
}

#establish what version of stroom-stats we are building
if [ -n "$TRAVIS_TAG" ]; then
    STROOM_STATS_VERSION="${TRAVIS_TAG}"
else
    #No tag so use the branch name as the version
    STROOM_STATS_VERSION="${TRAVIS_BRANCH}"
fi

#Dump all the travis env vars to the console for debugging
echo -e "TRAVIS_BUILD_NUMBER:  [${GREEN}${TRAVIS_BUILD_NUMBER}${NC}]"
echo -e "TRAVIS_COMMIT:        [${GREEN}${TRAVIS_COMMIT}${NC}]"
echo -e "TRAVIS_BRANCH:        [${GREEN}${TRAVIS_BRANCH}${NC}]"
echo -e "TRAVIS_TAG:           [${GREEN}${TRAVIS_TAG}${NC}]"
echo -e "TRAVIS_PULL_REQUEST:  [${GREEN}${TRAVIS_PULL_REQUEST}${NC}]"
echo -e "TRAVIS_EVENT_TYPE:    [${GREEN}${TRAVIS_EVENT_TYPE}${NC}]"
echo -e "STROOM_STATS_VERSION: [${GREEN}${STROOM_STATS_VERSION}${NC}]"

if [ "$TRAVIS_EVENT_TYPE" = "cron" ]; then
    echo "This is a cron build so just tag the commit and exit"
    echo "The build will happen when travis picks up the tagged commit"
    #This is a cron triggered build so tag as -DAILY and push a tag to git
    DATE_ONLY="$(date +%Y%m%d)"
    gitTag="${STROOM_STATS_VERSION}-${DATE_ONLY}-DAILY"
    
    createGitTag ${gitTag}
else
    #Do the gradle build
    #TODO need to find a way of running the int tests that doesn't blow the memory limit
    ./gradlew -Pversion=$TRAVIS_TAG clean build -x integrationTest

    if [ -n "$TRAVIS_TAG" ]; then
        SPECIFIC_TAG="--tag=${DOCKER_REPO}:${TRAVIS_TAG}"
        doDockerBuild=true
    elif [[ "$TRAVIS_BRANCH" =~ $BRANCH_WHITELIST_REGEX ]]; then
        FLOATING_TAG="--tag=${DOCKER_REPO}:${STROOM_STATS_VERSION}-SNAPSHOT"
        doDockerBuild=true
    fi

    echo -e "SPECIFIC DOCKER TAG: [${GREEN}${SPECIFIC_TAG}${NC}]"
    echo -e "FLOATING DOCKER TAG: [${GREEN}${FLOATING_TAG}${NC}]"
    echo -e "doDockerBuild:       [${GREEN}${doDockerBuild}${NC}]"

    #Don't do a docker build for pull requests
    if [ "$doDockerBuild" = true ] && [ "$TRAVIS_PULL_REQUEST" = "false" ] ; then
        echo -e "Building a docker image with tags: ${GREEN}${SPECIFIC_TAG}${NC} ${GREEN}${FLOATING_TAG}${NC}"

        #The username and password are configured in the travis gui
        docker login -u="$DOCKER_USERNAME" -p="$DOCKER_PASSWORD"
        docker build ${SPECIFIC_TAG} ${FLOATING_TAG} ${DOCKER_CONTEXT_ROOT}
        docker push ${DOCKER_REPO}
    fi
fi

exit 0
