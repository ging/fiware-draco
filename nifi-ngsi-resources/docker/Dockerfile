
FROM openjdk:8u191-jre-alpine3.9
LABEL maintainer="Andrés Muñoz<joseandres.munoz@upm.es>"
LABEL site="https://github.com/ging/fiware-draco"

ARG UID=1000
ARG GID=1000
ARG NIFI_VERSION=1.13.0
ARG BASE_URL=https://archive.apache.org/dist
ARG MIRROR_BASE_URL=${MIRROR_BASE_URL:-${BASE_URL}}
ARG NIFI_BINARY_PATH=${NIFI_BINARY_PATH:-/nifi/${NIFI_VERSION}/nifi-${NIFI_VERSION}-bin.zip}
ARG NIFI_TOOLKIT_BINARY_PATH=${NIFI_TOOLKIT_BINARY_PATH:-/nifi/${NIFI_VERSION}/nifi-toolkit-${NIFI_VERSION}-bin.zip}

ENV NIFI_BASE_DIR=/opt/nifi
ENV NIFI_HOME ${NIFI_BASE_DIR}/nifi-current
ENV NIFI_TOOLKIT_HOME ${NIFI_BASE_DIR}/nifi-toolkit-current

ENV NIFI_PID_DIR=${NIFI_HOME}/run
ENV NIFI_LOG_DIR=${NIFI_HOME}/logs

ENV DRACO_RELEASE=1.3.7
ENV NIFI_NGSI_NAR_VERSION=1.3.7

ADD sh/ ${NIFI_BASE_DIR}/scripts/

# Setup NiFi user and create necessary directories
# change uid and gid for elasticsearch user
RUN apk --no-cache add shadow

RUN addgroup -g ${GID} -S nifi || groupmod -n nifi `getent group ${GID} | cut -d: -f1` \
    && adduser -u ${UID} -S nifi -g ${GID} \
    && mkdir -p ${NIFI_BASE_DIR} \
    && chown -R nifi:nifi ${NIFI_BASE_DIR} \
    && apk update \
    && apk add jq xmlstarlet procps curl

USER nifi

# Download, validate, and expand Apache NiFi Toolkit binary.
RUN curl -fSL ${MIRROR_BASE_URL}/${NIFI_TOOLKIT_BINARY_PATH} -o ${NIFI_BASE_DIR}/nifi-toolkit-${NIFI_VERSION}-bin.zip \
    && echo "$(curl ${BASE_URL}/${NIFI_TOOLKIT_BINARY_PATH}.sha256) *${NIFI_BASE_DIR}/nifi-toolkit-${NIFI_VERSION}-bin.zip" | sha256sum -c - \
    && unzip ${NIFI_BASE_DIR}/nifi-toolkit-${NIFI_VERSION}-bin.zip -d ${NIFI_BASE_DIR} \
    && rm ${NIFI_BASE_DIR}/nifi-toolkit-${NIFI_VERSION}-bin.zip \
    && mv ${NIFI_BASE_DIR}/nifi-toolkit-${NIFI_VERSION} ${NIFI_TOOLKIT_HOME} \
    && ln -s ${NIFI_TOOLKIT_HOME} ${NIFI_BASE_DIR}/nifi-toolkit-${NIFI_VERSION}

# Download, validate, and expand Apache NiFi binary.
RUN curl -fSL ${MIRROR_BASE_URL}/${NIFI_BINARY_PATH} -o ${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}-bin.zip \
    && echo "$(curl ${BASE_URL}/${NIFI_BINARY_PATH}.sha256) *${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}-bin.zip" | sha256sum -c - \
    && unzip ${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}-bin.zip -d ${NIFI_BASE_DIR} \
    && rm ${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}-bin.zip \
    && mv ${NIFI_BASE_DIR}/nifi-${NIFI_VERSION} ${NIFI_HOME} \
    && mkdir -p ${NIFI_HOME}/conf \
    && mkdir -p ${NIFI_HOME}/database_repository \
    && mkdir -p ${NIFI_HOME}/flowfile_repository \
    && mkdir -p ${NIFI_HOME}/content_repository \
    && mkdir -p ${NIFI_HOME}/provenance_repository \
    && mkdir -p ${NIFI_HOME}/state \
    && mkdir -p ${NIFI_LOG_DIR} \
    && ln -s ${NIFI_HOME} ${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}

VOLUME ${NIFI_LOG_DIR} \
       ${NIFI_HOME}/conf \
       ${NIFI_HOME}/database_repository \
       ${NIFI_HOME}/flowfile_repository \
       ${NIFI_HOME}/content_repository \
       ${NIFI_HOME}/provenance_repository \
       ${NIFI_HOME}/state

COPY drivers/ ${NIFI_HOME}/drivers/
COPY templates/ ${NIFI_HOME}/conf/templates/

# Clear nifi-env.sh in favour of configuring all environment variables in the Dockerfile
RUN echo "#!/bin/sh\n" > $NIFI_HOME/bin/nifi-env.sh

# Web HTTP(s) & Socket Site-to-Site Ports
EXPOSE 8080 8443 10000 8000

WORKDIR ${NIFI_HOME}

RUN curl -L -o "nifi-ngsi-nar-${NIFI_NGSI_NAR_VERSION}.nar" "https://github.com/ging/fiware-draco/releases/download/${DRACO_RELEASE}/nifi-ngsi-nar-${NIFI_NGSI_NAR_VERSION}.nar"\
	&& cp ./nifi-ngsi-nar-${NIFI_NGSI_NAR_VERSION}.nar ${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}/lib/nifi-ngsi-nar-${NIFI_NGSI_NAR_VERSION}.nar

RUN curl -L -o "nifi-ngsi-cassandra-nar-${NIFI_NGSI_NAR_VERSION}.nar" "https://github.com/ging/fiware-draco/releases/download/${DRACO_RELEASE}/nifi-ngsi-cassandra-nar-${NIFI_NGSI_NAR_VERSION}.nar"\
	&& cp ./nifi-ngsi-cassandra-nar-${NIFI_NGSI_NAR_VERSION}.nar ${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}/lib/nifi-ngsi-cassandra-nar-${NIFI_NGSI_NAR_VERSION}.nar

RUN curl -L -o "nifi-ngsi-dynamo-nar-${NIFI_NGSI_NAR_VERSION}.nar" "https://github.com/ging/fiware-draco/releases/download/${DRACO_RELEASE}/nifi-ngsi-dynamo-nar-${NIFI_NGSI_NAR_VERSION}.nar"\
	&& cp ./nifi-ngsi-dynamo-nar-${NIFI_NGSI_NAR_VERSION}.nar ${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}/lib/nifi-ngsi-dynamo-nar-${NIFI_NGSI_NAR_VERSION}.nar

# Apply configuration and start NiFi
#
# We need to use the exec form to avoid running our command in a subshell and omitting signals,
# thus being unable to shut down gracefully:
# https://docs.docker.com/engine/reference/builder/#entrypoint
#
# Also we need to use relative path, because the exec form does not invoke a command shell,
# thus normal shell processing does not happen:
# https://docs.docker.com/engine/reference/builder/#exec-form-entrypoint-example
ENTRYPOINT ["../scripts/start.sh"]
