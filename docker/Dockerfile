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

FROM openjdk:8-alpine

ADD build/stroom-stats-all*.jar /stroom-stats/stroom-stats-all.jar
ADD build/config.yml /stroom-stats/
Add docker-entrypoint.sh /usr/local/bin/

# If we're using a VPN we'll need to set the proxies
RUN echo "http_proxy: $http_proxy" && \
    echo "https_proxy: $https_proxy" && \
    apk add --no-cache \
        bash \
        su-exec \
        tini && \
    adduser -s /bin/false -D stroomstats && \
    mkdir -p /stroom-stats

WORKDIR /stroom-stats

#Make sure all the files are owned by the stroom user
RUN chown -R stroomstats /stroom-stats

EXPOSE 8086 8087

#run entrypoint script inside tini for better unix process handling, see https://github.com/krallin/tini/issues/8
ENTRYPOINT ["/sbin/tini", "--", "docker-entrypoint.sh"]

#start the app
CMD ["java", "-jar", "stroom-stats-all.jar", "server", "config.yml"]
