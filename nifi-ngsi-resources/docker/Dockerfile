# Licensed to the Apache Software Foundation (ASF) under one 
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information 
# regarding copyright ownership. The ASF licenses this file 
# to you under the Apache License, Version 2.0 (the # "License"); you may not use this file except in compliance 
# with the License. You may obtain a copy of the License at 
# #   http://www.apache.org/licenses/LICENSE-2.0 # 
# Unless required by applicable law or agreed to in writing, 
# software distributed under the License is distributed on an 
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY 
# KIND, either express or implied. See the License for the 
# specific language governing permissions and limitations 
# under the License. # 
FROM openjdk:8-jre
LABEL maintainer="Andrés Muñoz <jamunoz@dit.upm.es>"
LABEL site="https://nifi.apache.org"
ARG UID=1000
ARG GID=1000
ARG NIFI_VERSION=1.7.0
ARG MIRROR=https://archive.apache.org/dist
ENV NIFI_BASE_DIR /opt/nifi
ENV NIFI_HOME=${NIFI_BASE_DIR}/nifi-${NIFI_VERSION} \
	NIFI_BINARY_URL=/nifi/${NIFI_VERSION}/nifi-${NIFI_VERSION}-bin.tar.gz
ENV NIFI_PID_DIR=${NIFI_HOME}/run
ENV NIFI_LOG_DIR=${NIFI_HOME}/logs
ENV Draco_HTTP_PORT=8080 \
    Draco_HTTP_PORT=8443 \
    Draco_REMOTE_INPUT=10000
ADD sh/ ${NIFI_BASE_DIR}/scripts/

# Setup NiFi user and create necessary directories 

RUN groupadd -g ${GID} nifi || groupmod -n nifi `getent group ${GID} | cut -d: -f1` \ 
	&& useradd --shell /bin/bash -u ${UID} -g ${GID} -m nifi \ 
	&& mkdir -p ${NIFI_HOME}/conf/templates \ 
	&& mkdir -p $NIFI_BASE_DIR/data \ 
	&& mkdir -p $NIFI_BASE_DIR/flowfile_repository \ 
	&& mkdir -p $NIFI_BASE_DIR/content_repository \ 
	&& mkdir -p $NIFI_BASE_DIR/provenance_repository \ 
	&& mkdir -p $NIFI_BASE_DIR/drivers \
	&& mkdir -p $NIFI_LOG_DIR \ 
	&& chown -R nifi:nifi ${NIFI_BASE_DIR} \ 
	&& apt-get update \ 
	&& apt-get install -y jq xmlstarlet procps
USER nifi 

# Download, validate, and expand Apache NiFi binary. 

RUN curl -fSL ${MIRROR}/${NIFI_BINARY_URL} -o ${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}-bin.tar.gz \ 
	&& echo "$(curl https://archive.apache.org/dist/${NIFI_BINARY_URL}.sha256) *${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}-bin.tar.gz" | sha256sum -c - \ 
	&& tar -xvzf ${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}-bin.tar.gz -C ${NIFI_BASE_DIR} \ 
	&& rm ${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}-bin.tar.gz

# 
 
# Clear nifi-env.sh in favour of configuring all environment variables in the Dockerfile 
RUN echo "#!/bin/sh\n" > $NIFI_HOME/bin/nifi-env.sh 
# Web HTTP(s) & Socket Site-to-Site Ports 

EXPOSE ${Draco_HTTP_PORT} ${Draco_HTTPS_PORT} ${Draco_REMOTE_INPUT}
WORKDIR ${NIFI_HOME} 
RUN curl -L -o "nifi-ngsi-resources.tar.gz" "https://github.com/ging/fiware-Draco/releases/download/FIWARE_7.4/nifi-ngsi-resources.tar.gz"\
	&& tar -xvzf nifi-ngsi-resources.tar.gz -C ./ \
	&& rm nifi-ngsi-resources.tar.gz \
	&& cp nifi-ngsi-resources/nifi-ngsi-nar-1.0-SNAPSHOT.nar ${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}/lib/nifi-ngsi-nar-1.0-SNAPSHOT.nar \
	&& cp -r nifi-ngsi-resources/drivers ./ \
	&& cp -r nifi-ngsi-resources/templates/. ${NIFI_HOME}/conf/templates

# Apply configuration and start NiFi # 
# We need to use the exec form to avoid running our command in a subshell and omitting signals, 
# thus being unable to shut down gracefully: 
# https://docs.docker.com/engine/reference/builder/#entrypoint # 
# Also we need to use relative path, because the exec form does not invoke a command shell, 
# thus normal shell processing does not happen: 
# https://docs.docker.com/engine/reference/builder/
#exec-form-entrypoint-example 

ENTRYPOINT ["../scripts/start.sh"]

