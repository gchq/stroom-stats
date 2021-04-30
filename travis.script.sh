#!/bin/bash

#exit script on any error
set -e

GITHUB_REPO="gchq/stroom-stats"
GITHUB_API_URL="https://api.github.com/repos/gchq/stroom-stats/releases"
STROOM_STATS_DOCKER_REPO="gchq/stroom-stats"
STROOM_STATS_HBASE_DOCKER_REPO="gchq/stroom-stats-hbase"
STROOM_STATS_DOCKER_CONTEXT_ROOT="docker/stroom-stats/."
STROOM_STATS_HBASE_DOCKER_CONTEXT_ROOT="docker/stroom-stats-hbase/."

VERSION_FIXED_TAG=""
SNAPSHOT_FLOATING_TAG=""
MAJOR_VER_FLOATING_TAG=""
MINOR_VER_FLOATING_TAG=""

HBASE_VERSION_SUFFIX="_hbase-v2.1" #TODO ideally we should pick this up dynamically so it is only defined in the gradle build
#This is a whitelist of branches to produce docker builds for
BRANCH_WHITELIST_REGEX='(^dev$|^master$|^v[0-9].*$)'
RELEASE_VERSION_REGEX='^v[0-9]+\.[0-9]+\.[0-9].*$'
CRON_TAG_SUFFIX="DAILY"
LATEST_SUFFIX="-LATEST"
doDockerBuild=false

# shellcheck disable=SC2034
{
    #Shell Colour constants for use in 'echo -e'
    #e.g.  echo -e "My message ${GREEN}with just this text in green${NC}"
    RED='\033[1;31m'
    GREEN='\033[1;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[1;34m'
    NC='\033[0m' # No Colour 
}

createGitTag() {
    tagName=$1
    
    git config --global user.email "builds@travis-ci.com"
    git config --global user.name "Travis CI"

    echo -e "Tagging commit [${GREEN}${TRAVIS_COMMIT}${NC}]" \
        "with tag [${GREEN}${tagName}${NC}]"
    git tag \
        -a \
        "${tagName}" \
        "${TRAVIS_COMMIT}" \
        -m "Automated Travis build $TRAVIS_BUILD_NUMBER" 
    #TAGPERM is a travis encrypted github token, see 'env' section in .travis.yml
    git push -q "https://$TAGPERM@github.com/${GITHUB_REPO}" "${tagName}"
}

isCronBuildRequired() {
    #GH_USER_AND_TOKEN is set in env section of .travis.yml
    if [ "x${GH_USER_AND_TOKEN}" = "x" ]; then 
        #no token so do it unauthenticated
        authArgs=()
    else
        echo "Using authentication with curl"
        authArgs=(
            "--user" 
            "${GH_USER_AND_TOKEN}"
        )
    fi
    #query the github api for the latest cron release tag name
    #redirect stderr to dev/null to protect api token
    latestTagName=$(curl -s "${authArgs[@]}" "${GITHUB_API_URL}" | \
        jq -r "[.[] | select(.tag_name | test(\"${TRAVIS_BRANCH}.*${CRON_TAG_SUFFIX}\"))][0].tag_name" 2>/dev/null)
    echo -e "Latest release ${CRON_TAG_SUFFIX} tag: [${GREEN}${latestTagName}${NC}]"

    true
    if [ "x${latestTagName}" != "x" ]; then 
        #Get the commit sha that this tag applies to (not the commit of the tag itself)
        shaForTag=$(git rev-list -n 1 "${latestTagName}")
        echo -e "SHA hash for tag ${latestTagName}: [${GREEN}${shaForTag}${NC}]"
        if [ "x${shaForTag}" = "x" ]; then
            echo -e "${RED}Unable to get sha for tag ${BLUE}${latestTagName}${NC}"
            exit 1
        else
            if [ "${shaForTag}x" = "${TRAVIS_COMMIT}x" ]; then
                echo -e "${RED}The commit of the build matches the latest ${CRON_TAG_SUFFIX} release.${NC}"
                echo -e "${RED}Git will not be tagged and no release will be made.${NC}"
                #The latest release has the same commit sha as the commit travis is building
                #so don't bother creating a new tag as we don't want a new release
                false
            fi
        fi
    else
        #no release found so return true so a build happens
        true
    fi
    return
}

#args: dockerRepo contextRoot tag1VersionPart tag2VersionPart ... tagNVersionPart
releaseToDockerHub() {
    #echo "releaseToDockerHub called with args [$@]"

    if [ $# -lt 3 ]; then
        echo "Incorrect args, expecting at least 3"
        exit 1
    fi
    dockerRepo="$1"
    contextRoot="$2"
    #shift the the args so we can loop round the open ended list of tags, $1 is now the first tag
    shift 2

    allTagArgs=()

    for tagVersionPart in "$@"; do
        if [ "x${tagVersionPart}" != "x" ]; then
            #echo -e "Adding docker tag [${GREEN}${tagVersionPart}${NC}]"
            allTagArgs+=( "--tag=${dockerRepo}:${tagVersionPart}" )
        fi
    done

    echo -e "Building and releasing a docker image to" \
        "${GREEN}${dockerRepo}${NC} with tags: ${GREEN}${allTagArgs[*]}${NC}"
    echo -e "dockerRepo:  [${GREEN}${dockerRepo}${NC}]"
    echo -e "contextRoot: [${GREEN}${contextRoot}${NC}]"

    #The username and password are configured in the travis gui
    docker login -u="$DOCKER_USERNAME" -p="$DOCKER_PASSWORD" >/dev/null 2>&1

    docker build "${allTagArgs[@]}" "${contextRoot}" >/dev/null 2>&1
    docker push "${dockerRepo}" >/dev/null 2>&1
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

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#TODO temporary code for debugging

#if [ "${GH_USER_AND_TOKEN}x" = "x" ]; then 
#    #no token so do it unauthenticated
#    authArgs=""
#else
#    echo "Using authentication with curl"
#    authArgs="--user ${GH_USER_AND_TOKEN}"
#fi
##query the github api for the latest cron release tag name
##redirect stderr to dev/null to protect api token
#echo -e "${RED}Outputting all release tags:${NC}"
#curl -s ${authArgs} ${GITHUB_API_URL} | \
#    jq -r "[.[] | select(.tag_name | test(\"${TRAVIS_BRANCH}.*${CRON_TAG_SUFFIX}\"))][].tag_name" 2>/dev/null
#
#echo -e "${RED}Outputting lates release:${NC}"
#curl -s ${authArgs} ${GITHUB_API_URL}/latest 
#
#latestTagName=$(curl -s ${authArgs} ${GITHUB_API_URL} | \
#    jq -r "[.[] | select(.tag_name | test(\"${TRAVIS_BRANCH}.*${CRON_TAG_SUFFIX}\"))][0].tag_name" 2>/dev/null)
#echo -e "Latest release ${CRON_TAG_SUFFIX} tag: [${GREEN}${latestTagName}${NC}]"

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


if [ "$TRAVIS_EVENT_TYPE" = "cron" ]; then
    echo "This is a cron build so just tag the commit (if required) and exit"

    if isCronBuildRequired; then
        echo "The build will happen when travis picks up the tagged commit"
        #This is a cron triggered build so tag as -DAILY and push a tag to git
        DATE_ONLY="$(date +%Y%m%d)"
        gitTag="${STROOM_STATS_VERSION}-${DATE_ONLY}-${CRON_TAG_SUFFIX}"
        
        createGitTag "${gitTag}"
    fi
else
    #Normal commit/PR/tag build
    extraBuildArgs=()

    if [ -n "$TRAVIS_TAG" ]; then
        doDockerBuild=true

        #This is a tagged commit, so create a docker image with that tag, e.g. v6.1.2
        VERSION_FIXED_TAG="${TRAVIS_TAG}"

        #Extract the major version part for a floating tag, e.g. v6-LATEST
        majorVer=$(echo "${TRAVIS_TAG}" | grep -oP "^v[0-9]+")
        if [ -n "${majorVer}" ]; then
            MAJOR_VER_FLOATING_TAG="${majorVer}${LATEST_SUFFIX}"
        fi

        #Extract the minor version part for a floating tag, e.g. v6.0-LATEST
        minorVer=$(echo "${TRAVIS_TAG}" | grep -oP "^v[0-9]+\.[0-9]+")
        if [ -n "${minorVer}" ]; then
            MINOR_VER_FLOATING_TAG="${minorVer}${LATEST_SUFFIX}"
        fi

        if [[ "$TRAVIS_BRANCH" =~ ${RELEASE_VERSION_REGEX} ]]; then
            echo "This is a release version so add gradle arg for publishing libs to Bintray"
            extraBuildArgs=( 
                "signMavenJavaPublication"
                "publishToSonatype"
                "closeSonatypeStagingRepository"
                #"closeAndReleaseSonatypeStagingRepository"
            )
        fi
            
    elif [[ "$TRAVIS_BRANCH" =~ $BRANCH_WHITELIST_REGEX ]]; then
        SNAPSHOT_FLOATING_TAG="${STROOM_STATS_VERSION}-SNAPSHOT"
        doDockerBuild=true
    fi

    echo -e "doDockerBuild:                 [${GREEN}${doDockerBuild}${NC}]"
    echo -e "extraBuildArgs:                [${GREEN}${extraBuildArgs[*]}${NC}]"
    echo -e "VERSION FIXED DOCKER TAG:      [${GREEN}${VERSION_FIXED_TAG}${NC}]"
    echo -e "SNAPSHOT FLOATING DOCKER TAG:  [${GREEN}${SNAPSHOT_FLOATING_TAG}${NC}]"
    echo -e "MAJOR VER FLOATING DOCKER TAG: [${GREEN}${MAJOR_VER_FLOATING_TAG}${NC}]"
    echo -e "MINOR VER FLOATING DOCKER TAG: [${GREEN}${MINOR_VER_FLOATING_TAG}${NC}]"

    #Do the gradle build
    #TODO need to find a way of running the int tests that doesn't blow the memory limit
    ./gradlew \
        -Pversion="${TRAVIS_TAG}" \
        clean \
        build \
        -x integrationTest \
        "${extraBuildArgs[@]}"

    #Don't do a docker build for pull requests
    if [ "$doDockerBuild" = true ] && [ "$TRAVIS_PULL_REQUEST" = "false" ] ; then

        allStatsTags=(
            "${VERSION_FIXED_TAG}"
            "${SNAPSHOT_FLOATING_TAG}"
            "${MAJOR_VER_FLOATING_TAG}"
            "${MINOR_VER_FLOATING_TAG}"
        )

        #build and release the stroom-stats image to dockerhub
        releaseToDockerHub \
            "${STROOM_STATS_DOCKER_REPO}" \
            "${STROOM_STATS_DOCKER_CONTEXT_ROOT}" \
            "${allStatsTags[@]}"

        #build all the docker tags for the hbase image
        allHbaseTags=()
        [ -n "${VERSION_FIXED_TAG}" ] \
            && allHbaseTags+=( "${VERSION_FIXED_TAG}${HBASE_VERSION_SUFFIX}" )
        [ -n "${SNAPSHOT_FLOATING_TAG}" ] \
            && allHbaseTags+=( "${SNAPSHOT_FLOATING_TAG}${HBASE_VERSION_SUFFIX}" )
        [ -n "${MAJOR_VER_FLOATING_TAG}" ] \
            && allHbaseTags+=( "${MAJOR_VER_FLOATING_TAG}${HBASE_VERSION_SUFFIX}" )
        [ -n "${MINOR_VER_FLOATING_TAG}" ] \
            && allHbaseTags+=( "${MINOR_VER_FLOATING_TAG}${HBASE_VERSION_SUFFIX}" )

        #Build and release the hbase image to dockerhub with the stroom-stats filter already in it
        releaseToDockerHub \
            "${STROOM_STATS_HBASE_DOCKER_REPO}" \
            "${STROOM_STATS_HBASE_DOCKER_CONTEXT_ROOT}" \
            "${allHbaseTags[@]}"
    fi
fi

exit 0
# vim: set tabstop=4 shiftwidth=4 expandtab:
