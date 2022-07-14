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
package org.apache.nifi.processors.ngsi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.ngsi.ngsi.backends.ckan.vocabularies.Availability;
import org.apache.nifi.processors.ngsi.ngsi.backends.ckan.vocabularies.ResourceFormats;
import org.json.simple.JSONArray;

@Tags({ "ckan", "metadata" })
@CapabilityDescription("CKAN metadata generator complian with DCAT-AP 2.0.1")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class UpdateCkanMetadataAttributes extends AbstractProcessor {

        public static final PropertyDescriptor ORGANIZATION_NAME = new PropertyDescriptor.Builder()
                        .name("organizationName")
                        .displayName("Organization name")
                        .description("Organization name. Example: exampleorg")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                        .build();

        public static final PropertyDescriptor ORGANIZATION_TYPE = new PropertyDescriptor.Builder()
                        .name("organizationType")
                        .displayName("Organization type")
                        .description("Organization name. Example: http://purl.org/adms/publishertype/Academia-ScientificOrganisation")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                        .build();

        public static final PropertyDescriptor PACKAGE_DESCRIPTION = new PropertyDescriptor.Builder()
                        .name("packageDescription")
                        .displayName("Package description")
                        .description("Package description. Example: Description example of dataset test")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                        .build();

        public static final PropertyDescriptor PACKAGE_NAME = new PropertyDescriptor.Builder().name("packageName")
                        .displayName("Package name")
                        .description("Organization name. Example: exampledataset")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                        .build();

        public static final PropertyDescriptor CONTACT_POINT = new PropertyDescriptor.Builder().name("contactPoint")
                        .displayName("Contact point uri")
                        .description("Contact point. Example: http://example.com/ContactUri")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.URI_VALIDATOR)
                        .build();

        public static final PropertyDescriptor CONTACT_NAME = new PropertyDescriptor.Builder().name("contactName")
                        .displayName("Contact name")
                        .description("Contact name. Example: MaintainerName ExampleSurname")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                        .build();

        public static final PropertyDescriptor CONTACT_EMAIL = new PropertyDescriptor.Builder().name("contactEmail")
                        .displayName("Contact email")
                        .description("Contact email. Example: maintainer@email.com")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                        .build();

        public static final PropertyDescriptor KEYWORDS = new PropertyDescriptor.Builder().name("keywords")
                        .displayName("Keywords")
                        .description("A comma delimited list of keywords. Example: newTag1,newTag2")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                        .build();

        public static final PropertyDescriptor PUBLISHER_URL = new PropertyDescriptor.Builder().name("publisherURL")
                        .displayName("Publisher URL")
                        .description("The publisher URL, it is recommended to use the controlled vocabulary "
                                        + "proposed by the DCAT-AP (http://publications.europa.eu/resource/authority/corporate-body). "
                                        + "Example: http://example.com/publisher")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.URI_VALIDATOR)
                        .build();

        public static final PropertyDescriptor SPATIAL_URI = new PropertyDescriptor.Builder().name("spatialURI")
                        .displayName("Spatial URI")
                        .description("The spatial URI coverage, it is recommended to use the controlled vocabularies "
                                        + "(http://publications.europa.eu/resource/authority/continent/,"
                                        + "http://publications.europa.eu/resource/authority/country,"
                                        + " http://publications.europa.eu/resource/authority/place/)"
                                        + "Example: https://publications.europa.eu/resource/authority/country/ESP")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.URI_VALIDATOR)
                        .build();

        public static final PropertyDescriptor SPATIAL_COVERAGE = new PropertyDescriptor.Builder()
                        .name("spatialCoverage")
                        .displayName("Spatial coverage")
                        .description("The spatial coverage. It could be a Point or a set of Points (a POLIGON). "
                                        + "If it is a Point the coordinaes sepparated by comma inside brackets. "
                                        + "If it is a set of Points (polygon) the points separated by comma inside two blocks of brackets "
                                        + "Example1: [175.0, 17.5]"
                                        + "Example2: [[[175.0, 17.5], [-65.5, 17.5], [-80.5, 10.5]]]")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                        .build();

        public static final PropertyDescriptor TEMPORAL_START = new PropertyDescriptor.Builder().name("temporalStart")
                        .displayName("Temporal start")
                        .description("Temporal coverage start in format yyyy-MM-dd-THH:mm:ss.SSSZ "
                                        + "Example: 2021-05-25T15:01:07.173")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.ISO8601_INSTANT_VALIDATOR)
                        .build();

        public static final PropertyDescriptor TEMPORAL_END = new PropertyDescriptor.Builder().name("temporalEnd")
                        .displayName("Temporal end")
                        .description("Temporal coverage end in format yyyy-MM-dd-THH:mm:ss.SSSZ "
                                        + "Example: 2021-05-25T15:01:07.173")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.ISO8601_INSTANT_VALIDATOR)
                        .build();

        public static final PropertyDescriptor THEMES = new PropertyDescriptor.Builder().name("themes")
                        .displayName("Themes")
                        .description("A comma delimited list of themes. It is recommended to select "
                                        + "them from the controlled vocabulary "
                                        + "(http://publications.europa.eu/resource/authority/data-theme). "
                                        + "Example: http://publications.europa.eu/resource/authority/data-theme/ECON,"
                                        + "http://publications.europa.eu/resource/authority/data-theme/EDUC")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                        .build();

        public static final PropertyDescriptor VERSION = new PropertyDescriptor.Builder().name("version")
                        .displayName("Version")
                        .description("Version. Example: 2.0")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                        .build();

        public static final PropertyDescriptor LANDING_PAGE = new PropertyDescriptor.Builder().name("landingPage")
                        .displayName("Landing page")
                        .description("Landing page of the package"
                                        + "Example: http://example.com/landing_page")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.URI_VALIDATOR)
                        .build();

        public static final PropertyDescriptor VISIBILITY = new PropertyDescriptor.Builder().name("visibility")
                        .displayName("Visibility")
                        .description("dataset visibility public or private")
                        .required(false)
                        .allowableValues("public", "private")
                        .build();

        public static final PropertyDescriptor DATASET_RIGHTS = new PropertyDescriptor.Builder().name("datasetRights")
                        .displayName("Dataset Rights")
                        .description("Dataset Rights. Example: http://publications.europa.eu/resource/authority/access-right/RESTRICTED")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.URI_VALIDATOR)
                        .build();

        public static final PropertyDescriptor ACCESS_URL = new PropertyDescriptor.Builder().name("accessURL")
                        .displayName("Access URL")
                        .description("The access URL, "
                                        + "Example: http://example.com/access_url")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.URI_VALIDATOR)
                        .build();

        public static final PropertyDescriptor AVAILABILITY = new PropertyDescriptor.Builder().name("availability")
                        .displayName("Availability")
                        .description("Package availability, it is recommended to use the controlled vocabulary "
                                        + "proposed by the DCAT-AP (http://dcat-ap.de/def/plannedAvailability/1_0.html). "
                                        + "Example: http://dcat-ap.de/def/plannedAvailability/stable")
                        .required(false)
                        .allowableValues(Availability.getAvailaibilityNames())
                        .build();

        public static final PropertyDescriptor RESOURCE_DESCRIPTION = new PropertyDescriptor.Builder()
                        .name("resourceDescription")
                        .displayName("Description of the resource")
                        .description("Description of the resource. "
                                        + "Example: Example description of example resource")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                        .build();

        public static final PropertyDescriptor FORMAT = new PropertyDescriptor.Builder().name("format")
                        .displayName("File format")
                        .description("Resource format. Example: JSON")
                        .required(false)
                        .allowableValues(ResourceFormats.getFormatNames())
                        .build();

        public static final PropertyDescriptor MIME_TYPE = new PropertyDescriptor.Builder().name("mimetype")
                        .displayName("Mimetype")
                        .description("Resource media type. Example: application/json")
                        .required(false)
                        .allowableValues(ResourceFormats.getMimetypes())
                        .build();

        public static final PropertyDescriptor LICENSE = new PropertyDescriptor.Builder().name("license")
                        .displayName("Resource license")
                        .description("Resource license, it is recommended to use one from "
                                        + "Creative Commons (http://creativecommons.org/licenses/), and in"
                                        + "particular the CC Zero Public Domain Dedication "
                                        + "(http://creativecommons.org/publicdomain/zero/1.0/)"
                                        + "Example: https://creativecommons.org/publicdomain/zero/1.0/")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                        .build();

        public static final PropertyDescriptor LICENSE_TYPE = new PropertyDescriptor.Builder().name("licenseType")
                        .displayName("Resource license type")
                        .description("It is recommended to use a license type from the controlled vocabulary "
                                        + "proposed by the DCAT-AP (http://purl.org/adms/licencetype/). "
                                        + "Example: http://purl.org/adms/licencetype/PublicDomain")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                        .build();

        public static final PropertyDescriptor DOWNLOAD_URL = new PropertyDescriptor.Builder().name("downloadURL")
                        .displayName("Download URL")
                        .description("The download URL. "
                                        + "Example: http://example.com/download_url")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.URI_VALIDATOR)
                        .build();

        public static final PropertyDescriptor BYTE_SIZE = new PropertyDescriptor.Builder().name("byteSize")
                        .displayName("File size in bytes")
                        .description("Number of bytes that occupies a file "
                                        + "Example: 200000000")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
                        .build();

        public static final PropertyDescriptor RESOURCE_NAME = new PropertyDescriptor.Builder().name("resourceName")
                        .displayName("Resource name")
                        .description("Resource name"
                                        + "Example: resourceName")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                        .build();
        
        public static final PropertyDescriptor RESOURCE_RIGHTS = new PropertyDescriptor.Builder().name("resourceRights")
                        .displayName("Resource Rights")
                        .description("Resource Rights. Example: http://publications.europa.eu/resource/authority/access-right/RESTRICTED")
                        .required(false)
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.URI_VALIDATOR)
                        .build();

        public static final Relationship REL_SUCCESS = new Relationship.Builder()
                        .name("Success")
                        .description("Created CKAN properties")
                        .build();

        private List<PropertyDescriptor> descriptors;

        private Set<Relationship> relationships;

        @Override
        protected void init(final ProcessorInitializationContext context) {
                final List<PropertyDescriptor> descriptors = new ArrayList<>();
                descriptors.add(ORGANIZATION_NAME);
                descriptors.add(ORGANIZATION_TYPE);
                descriptors.add(PACKAGE_DESCRIPTION);
                descriptors.add(PACKAGE_NAME);
                descriptors.add(CONTACT_POINT);
                descriptors.add(CONTACT_NAME);
                descriptors.add(CONTACT_EMAIL);
                descriptors.add(KEYWORDS);
                descriptors.add(PUBLISHER_URL);
                descriptors.add(SPATIAL_URI);
                descriptors.add(SPATIAL_COVERAGE);
                descriptors.add(TEMPORAL_START);
                descriptors.add(TEMPORAL_END);
                descriptors.add(THEMES);
                descriptors.add(VERSION);
                descriptors.add(LANDING_PAGE);
                descriptors.add(VISIBILITY);
                descriptors.add(DATASET_RIGHTS);
                descriptors.add(ACCESS_URL);
                descriptors.add(AVAILABILITY);
                descriptors.add(RESOURCE_DESCRIPTION);
                descriptors.add(FORMAT);
                descriptors.add(MIME_TYPE);
                descriptors.add(LICENSE);
                descriptors.add(LICENSE_TYPE);
                descriptors.add(DOWNLOAD_URL);
                descriptors.add(BYTE_SIZE);
                descriptors.add(RESOURCE_NAME);
                descriptors.add(RESOURCE_RIGHTS);
                this.descriptors = Collections.unmodifiableList(descriptors);

                final Set<Relationship> relationships = new HashSet<>();
                relationships.add(REL_SUCCESS);
                this.relationships = Collections.unmodifiableSet(relationships);
        }

        @Override
        public Set<Relationship> getRelationships() {
                return this.relationships;
        }

        @Override
        public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
                return descriptors;
        }

        @OnScheduled
        public void onScheduled(final ProcessContext context) {

        }

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
                FlowFile flowFile = session.get();
                if (flowFile == null) {
                        return;
                }
                final Map<String, String> attributesToUpdate = new HashMap<>();

                for (final PropertyDescriptor propertyDescriptor : getSupportedPropertyDescriptors()) {

                        String propertyValue = context.getProperty(propertyDescriptor)
                                        .evaluateAttributeExpressions(flowFile).getValue();

                        if (propertyValue != null) {
                                if (propertyDescriptor.equals(KEYWORDS)) {
                                        List<String> keywords_list = Arrays.asList(propertyValue.split(","));
                                        propertyValue = JSONArray.toJSONString(keywords_list);
                                } else if (propertyDescriptor.equals(THEMES)) {
                                        List<String> themes_list = Arrays.asList(propertyValue.split(","));
                                        propertyValue = JSONArray.toJSONString(themes_list);
                                }
                                attributesToUpdate.put(propertyDescriptor.getName(), propertyValue);
                        }

                }
                FlowFile returnFlowfile = session.putAllAttributes(flowFile, attributesToUpdate);

                session.getProvenanceReporter().modifyAttributes(returnFlowfile);
                session.transfer(returnFlowfile, REL_SUCCESS);
        }
}
