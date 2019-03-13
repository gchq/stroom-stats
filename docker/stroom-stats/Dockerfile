#**********************************************************************
# Copyright 2016 Crown Copyright
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#**********************************************************************

# The JDK (rather than JRE) is required for the diagnostic tools like
# jstat/jmap/jcmd/etc.
FROM openjdk:8u191-jdk-alpine3.9

WORKDIR /stroom-stats

EXPOSE 8086 8087

#run entrypoint script inside tini for better unix process handling, see https://github.com/krallin/tini/issues/8
ENTRYPOINT ["/sbin/tini", "--", "docker-entrypoint.sh"]

#start the app
# we want variable expansion of JAVA_OPTS
CMD ["sh", "-c", "echo \"JAVA_OPTS: [${JAVA_OPTS}]\"; java ${JAVA_OPTS} -jar stroom-stats-service-all.jar server config.yml"]

# If we're using a VPN we'll need to set the proxies
RUN echo "http_proxy: $http_proxy" && \
    echo "https_proxy: $https_proxy" && \
    apk add --no-cache \
        bash \
        su-exec \
        tini && \
    addgroup -g 1000 -S stroomstats && \
    adduser -u 1000 -S -s /bin/false -D -G stroomstats stroomstats && \
    mkdir -p /stroom-stats && \
    chown -R stroom:stroom /stroom-stats

ADD docker-entrypoint.sh /usr/local/bin/
ADD build/config.yml /stroom-stats/
ADD build/stroom-stats-service*-all.jar /stroom-stats/stroom-stats-service-all.jar

