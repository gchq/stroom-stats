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

WORKDIR /usr/stroom-stats

ADD ./stroom-stats-service/build/libs/stroom-stats-*-all.jar ./stroom-stats-all.jar
ADD ./stroom-stats-service/config.yml ./config.yml

# If we're using a VPN we'll need to set the proxies
RUN echo "http_proxy: $http_proxy" && \
    echo "https_proxy: $https_proxy" && \
    # Make sure everything is up-to-date
    apk update && \
    apk upgrade && \
    # Alpine doesn't come with bash so install it
    apk add --no-cache bash && \
    rm -rf /var/cache/apk/*

EXPOSE 8086 8087

CMD java -jar stroom-stats-all.jar server config.yml
