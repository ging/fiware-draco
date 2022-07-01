/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.ngsi.dynamo;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.processors.ngsi.ngsi.utils.Entity;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.ngsi.dynamo.backends.DynamoBackend;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSIEvent;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSIUtils;

import java.util.*;


@EventDriven
@Tags({"NGSI","FIWARE" ,"dynamodb", "insert", "update", "write", "put" })
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Writes the contents of a FlowFile to DynamoDB")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@SeeAlso(AbstractAWSCredentialsProviderProcessor.class)

public class NGSIToDynamo extends AbstractAWSCredentialsProviderProcessor<AmazonDynamoDBClient> {

    protected volatile DynamoDB dynamoDB;
    private static final DynamoBackend dynamoBackend = new DynamoBackend();


    static final PropertyDescriptor NGSI_VERSION = new PropertyDescriptor.Builder()
            .name("NGSI version")
            .displayName("NGSI version")
            .description("List of supported versions of NGSI (v2 and ld), currently only support v2")
            .required(false)
            .allowableValues("v2")
            .defaultValue("v2")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor DATA_MODEL = new PropertyDescriptor.Builder()
            .name("Data model")
            .displayName("Data Model")
            .description("The data model for creating the tables when an event have been received you can choose between: db-by-service-path or db-by-entity, default value is db-by-service-path")
            .required(false)
            .allowableValues("db-by-entity", "db-by-service-path")
            .defaultValue("db-by-entity")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor ATTRIBUTE_PERSISTENCE = new PropertyDescriptor.Builder()
            .name("Attribute persistence")
            .displayName("Attribute persistence")
            .description("The mode of storing the data inside of the table")
            .required(false)
            .allowableValues("row", "column")
            .defaultValue("row")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor DEFAULT_SERVICE = new PropertyDescriptor.Builder()
            .name("Default service")
            .displayName("Default service")
            .description("In case you don't set the Fiware-Service header in the context broker, this value will be used as Fiware-Service")
            .required(false)
            .defaultValue("test")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor DEFAULT_SERVICE_PATH = new PropertyDescriptor.Builder()
            .name("Default service path")
            .displayName("Default service path")
            .description("In case you don't set the Fiware-ServicePath header in the context broker, this value will be used as Fiware-ServicePath")
            .required(false)
            .defaultValue("/path")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor ENABLE_ENCODING= new PropertyDescriptor.Builder()
            .name("enable-encoding")
            .displayName("Enable Encoding")
            .description("true or false, true applies the new encoding, false applies the old encoding.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    static final PropertyDescriptor ENABLE_LOWERCASE= new PropertyDescriptor.Builder()
            .name("enable-lowercase")
            .displayName("Enable Lowercase")
            .description("true or false, true for creating the Schema and Tables name with lowercase.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch size")
            .displayName("Batch size")
            .description("Number of events accumulated before persistence (Maximum 25, check Amazon Web Services Documentation for more information).")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor BATCH_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Batch timeout")
            .displayName("Batch timeout")
            .description("Number of seconds the batch will be building before it is persisted as it is.")
            .required(false)
            .allowableValues("30")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor BATCH_TTL = new PropertyDescriptor.Builder()
            .name("Batch TTL")
            .displayName("Batch TTL")
            .description("Number of retries when a batch cannot be persisted. Use 0 for no retries, -1 for infinite retries. Please, consider an infinite TTL (even a very large one) may consume all the sink's channel capacity very quickly.")
            .required(false)
            .allowableValues("10")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor BATCH_RETRY_INTERVALS = new PropertyDescriptor.Builder()
            .name("Batch retry intervals")
            .displayName("Batch retry intervals")
            .description("Comma-separated list of intervals (in miliseconds) at which the retries regarding not persisted batches will be done. First retry will be done as many miliseconds after as the first value, then the second retry will be done as many miliseconds after as second value, and so on. If the batch_ttl is greater than the number of intervals, the last interval is repeated.")
            .required(false)
            .allowableValues("5000")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();


    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after the database is successfully updated")
            .build();
    static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the database cannot be updated and retrying the operation will also fail, "
                    + "such as an invalid query or an integrity constraint violation")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(NGSI_VERSION);
        properties.add(DATA_MODEL);
        properties.add(ATTRIBUTE_PERSISTENCE);
        properties.add(DEFAULT_SERVICE);
        properties.add(DEFAULT_SERVICE_PATH);
        properties.add(ENABLE_ENCODING);
        properties.add(ENABLE_LOWERCASE);
        properties.add(BATCH_SIZE);
        properties.add(BATCH_TIMEOUT);
        properties.add(BATCH_TTL);
        properties.add(BATCH_RETRY_INTERVALS);
        properties.add(ACCESS_KEY);
        properties.add(SECRET_KEY);
        properties.add(REGION);
        properties.add(CREDENTIALS_FILE);
        properties.add(AWS_CREDENTIALS_PROVIDER_SERVICE);
        properties.add(TIMEOUT);
        properties.add(SSL_CONTEXT_SERVICE);
        properties.add(PROXY_HOST);
        properties.add(PROXY_HOST_PORT);
        properties.add(PROXY_USERNAME);
        properties.add(PROXY_PASSWORD);
        properties.add(ENDPOINT_OVERRIDE);

        return properties;
    }

    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_RETRY);
        rels.add(REL_FAILURE);
        return rels;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        if ( flowFile == null ) {
            return;
        }

        ComponentLog logger = getLogger();

        final DynamoDB dynamoDB = getDynamoDB(context);
        dynamoBackend.setDynamoDB(dynamoDB);
        NGSIUtils n = new NGSIUtils();

        final NGSIEvent event=n.getEventFromFlowFile(flowFile,session,context.getProperty(NGSI_VERSION).getValue(), false);
        final String attrPersistence = context.getProperty(ATTRIBUTE_PERSISTENCE).getValue();
        final long creationTime = event.getCreationTime();
        final String fiwareService = (event.getFiwareService().compareToIgnoreCase("nd")==0)?context.getProperty(DEFAULT_SERVICE).getValue():event.getFiwareService();
        final String fiwareServicePath = (event.getFiwareServicePath().compareToIgnoreCase("/nd")==0)?context.getProperty(DEFAULT_SERVICE_PATH).getValue():event.getFiwareServicePath();

        try {
            for (Entity entity: event.getEntities()){
                logger.debug("Processing entity: "+entity.getEntityId());
                String tableName = dynamoBackend.buildTableName(fiwareServicePath, entity, context.getProperty(DATA_MODEL).getValue(), context.getProperty(ENABLE_ENCODING).asBoolean(), context.getProperty(ENABLE_LOWERCASE).asBoolean(), logger);
                dynamoBackend.createTable(tableName, logger);
                dynamoBackend.putItems(tableName, entity,creationTime,fiwareServicePath, attrPersistence, logger);
                logger.debug("Succesful posted items  of "+entity.getEntityId()+" to dynamodb table: "+tableName);
            }
            session.transfer(flowFile, REL_SUCCESS);

        } catch (AmazonServiceException exception) {
            logger.error("Could not process flowFiles due to service exception : " + exception.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        } catch (AmazonClientException exception) {
            logger.error("Could not process flowFiles due to client exception : " + exception.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }catch (Exception e) {
            logger.error("Could not process flowFiles due to exception : "+e.toString());
            session.transfer(flowFile, REL_FAILURE);
        }  //end try-catch
    }


    protected synchronized DynamoDB getDynamoDB(ProcessContext context) {
        if (dynamoDB==null)
            super.onScheduled(context);
        dynamoDB = new DynamoDB(client);
        return dynamoDB;
    }


    /**
     * Create client using credentials provider. This is the preferred way for creating clients
     */
    @Override
    protected AmazonDynamoDBClient createClient(final ProcessContext context, final AWSCredentialsProvider credentialsProvider, final ClientConfiguration config) {
        getLogger().debug("Creating client with credentials provider");
        final AmazonDynamoDBClient client = new AmazonDynamoDBClient(credentialsProvider, config);
        return client;
    }

    /**
     * Create client using AWSCredentials
     *
     * @deprecated use {@link #createClient(ProcessContext, AWSCredentialsProvider, ClientConfiguration)} instead
     */
    @Override
    protected AmazonDynamoDBClient createClient(final ProcessContext context, final AWSCredentials credentials, final ClientConfiguration config) {
        getLogger().debug("Creating client with aws credentials");
        final AmazonDynamoDBClient client = new AmazonDynamoDBClient(credentials, config);
        return client;
    }

    @OnStopped
    public void onStopped() {
        this.dynamoDB = null;
    }
}
