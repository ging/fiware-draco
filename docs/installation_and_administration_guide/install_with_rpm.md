# Installing Draco from package (Linux)

Simply configure the FIWARE Env variables:

```text
 MIRROR=https://archive.apache.org/dist \ NIFI_VERSION=1.7.0 \
 NIFI_BASE_DIR=/opt/nifi \
 NIFI_HOME=${NIFI_BASE_DIR}/nifi-${NIFI_VERSION} \ NIFI_BINARY_URL=/nifi/${NIFI_VERSION}/nifi-${NIFI_VERSION}-bin.tar.gz
\ NIFI_LOG_DIR=\${NIFI_HOME}/logs
```

Then download and decompress the package in the NIFI_HOME

```bash
curl -fSL ${MIRROR}/${NIFI_BINARY_URL} -o ${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}-bin.tar.gz \
	echo "$(curl https://archive.apache.org/dist/${NIFI_BINARY_URL}.sha256) *${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}-bin.tar.gz" | sha256sum -c - \
	tar -xvzf ${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}-bin.tar.gz -C ${NIFI_BASE_DIR} \
	rm ${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}-bin.tar.gz
```

Now, download the last release the fiware-Draco processors from the git hub repository

```bash
cd NIFI_HOME \
curl -L -o "nifi-ngsi-resources.tar.gz" "https://github.com/ging/fiware-Draco/releases/download/v2.0.0/nifi-ngsi-resources.tar.gz"\
	 tar -xvzf nifi-ngsi-resources.tar.gz -C ./ \
	 rm nifi-ngsi-resources.tar.gz \
	 cp nifi-ngsi-resources/nifi-ngsi-nar-1.0-SNAPSHOT.nar ${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}/lib/nifi-ngsi-nar-1.0-SNAPSHOT.nar \
	 cp -r nifi-ngsi-resources/drivers ./ \
	 cp -r nifi-ngsi-resources/templates ${NIFI_HOME}/conf
```

To run NiFi in the background, use bin/nifi.sh start. This will initiate the application to begin running.

```bash
cd NIFI_HOME \
 ./bin/nifi.sh start
```

To check the status and see if NiFi is currently running, execute the command .

```bash
 ./bin/nifi.sh status
```

NiFi can be shutdown by executing the command.

```bash
 ./bin/nifi.sh stop
```

With the service running, you can access to the NiFi GUI using this link `https://localhost:8443/nifi`
