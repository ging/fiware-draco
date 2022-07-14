package org.apache.nifi.processors.ngsi.ngsi.utils;

import com.google.gson.JsonArray;

import java.util.ArrayList;

public class DCATMetadata {
    private String organizationName;
    private String organizationType;
    private String packageDescription;
    private String packageName;
    private String contactPoint;
    private String contactName;
    private String contactEmail;
    private String [] keywords;
    private String publisherURL;
    private String spatialUri;
    private String spatialCoverage;
    private String temporalStart;
    private String temporalEnd;
    private String themes;
    private String version;
    private String landingPage;
    private String visibility;
    private String datasetRights;
    private String accessURL;
    private String availability;
    private String resourceDescription;
    private String format;
    private String mimeType;
    private String license;
    private String licenseType;
    private String downloadURL;
    private String byteSize;
    private String resourceName;
    private String resourceRights;

    public void DCATMetadata(){

    }

    public DCATMetadata(String organizationName, String organizationType, String packageDescription, String packageName, String contactPoint, String contactName, String contactEmail, String [] keywords, String publisherURL, String spatialUri, String spatialCoverage, String temporalStart, String temporalEnd, String themes, String version, String landingPage, String visibility, String datasetRights, String accessURL, String availability, String resourceDescription, String format, String mimeType, String license, String licenseType, String downloadURL, String byteSize, String resourceName, String resourceRights) {
        this.organizationName = organizationName;
        this.organizationType = organizationType;
        this.packageDescription = packageDescription;
        this.packageName = packageName;
        this.contactPoint = contactPoint;
        this.contactName = contactName;
        this.contactEmail = contactEmail;
        this.keywords = keywords;
        this.publisherURL = publisherURL;
        this.spatialUri = spatialUri;
        this.spatialCoverage = spatialCoverage;
        this.temporalStart = temporalStart;
        this.temporalEnd = temporalEnd;
        this.themes = themes;
        this.version = version;
        this.landingPage = landingPage;
        this.visibility = visibility;
        this.datasetRights = datasetRights;
        this.accessURL = accessURL;
        this.availability = availability;
        this.resourceDescription = resourceDescription;
        this.format = format;
        this.mimeType = mimeType;
        this.license = license;
        this.licenseType = licenseType;
        this.downloadURL = downloadURL;
        this.byteSize = byteSize;
        this.resourceName = resourceName;
        this.resourceRights = resourceRights;
    }

    public String getOrganizationName() {
        return organizationName;
    }

    public void setOrganizationName(String organizationName) {
        this.organizationName = organizationName;
    }

    public String getOrganizationType() {
        return organizationType;
    }

    public void setOrganizationType(String organizationType) {
        this.organizationType = organizationType;
    }

    public String getPackageDescription() {
        return packageDescription;
    }

    public void setPackageDescription(String packageDescription) {
        this.packageDescription = packageDescription;
    }

    public String getPackageName() {
        return packageName;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public String getContactPoint() {
        return contactPoint;
    }

    public String getSpatialUri() {
        return spatialUri;
    }

    public void setSpatialUri(String spatialUri) {
        this.spatialUri = spatialUri;
    }

    public void setContactPoint(String contactPoint) {
        this.contactPoint = contactPoint;
    }

    public String getContactName() {
        return contactName;
    }

    public void setContactName(String contactName) {
        this.contactName = contactName;
    }

    public String getContactEmail() {
        return contactEmail;
    }

    public void setContactEmail(String contactEmail) {
        this.contactEmail = contactEmail;
    }

    public String [] getKeywords() {
        return keywords;
    }

    public void setKeywords(String [] keywords) {
        this.keywords = keywords;
    }

    public String getPublisherURL() {
        return publisherURL;
    }

    public void setPublisherURL(String publisherURL) {
        this.publisherURL = publisherURL;
    }

    public String getSpatialCoverage() {
        return spatialCoverage;
    }

    public void setSpatialCoverage(String spatialCoverage) {
        this.spatialCoverage = spatialCoverage;
    }

    public String getTemporalStart() {
        return temporalStart;
    }

    public void setTemporalStart(String temporalStart) {
        this.temporalStart = temporalStart;
    }

    public String getTemporalEnd() {
        return temporalEnd;
    }

    public void setTemporalEnd(String temporalEnd) {
        this.temporalEnd = temporalEnd;
    }

    public String getThemes() {
        return themes;
    }

    public void setThemes(String themes) {
        this.themes = themes;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getLandingPage() {
        return landingPage;
    }

    public void setLandingPage(String landingPage) {
        this.landingPage = landingPage;
    }

    public String getVisibility() {
        return visibility;
    }

    public void setVisibility(String visibility) {
        this.visibility = visibility;
    }

    public String getDatasetRights() {
        return datasetRights;
    }

    public void setDatasetRights(String datasetRights) {
        this.datasetRights = datasetRights;
    }

    public String getAccessURL() {
        return accessURL;
    }

    public void setAccessURL(String accessURL) {
        this.accessURL = accessURL;
    }

    public String getAvailability() {
        return availability;
    }

    public void setAvailability(String availability) {
        this.availability = availability;
    }

    public String getResourceDescription() {
        return resourceDescription;
    }

    public void setResourceDescription(String resourceDescription) {
        this.resourceDescription = resourceDescription;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getMimeType() {
        return mimeType;
    }

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }

    public String getLicense() {
        return license;
    }

    public void setLicense(String license) {
        this.license = license;
    }

    public String getLicenseType() {
        return licenseType;
    }

    public void setLicenseType(String licenseType) {
        this.licenseType = licenseType;
    }

    public String getDownloadURL() {
        return downloadURL;
    }

    public void setDownloadURL(String downloadURL) {
        this.downloadURL = downloadURL;
    }

    public String getByteSize() {
        return byteSize;
    }

    public void setByteSize(String byteSize) {
        this.byteSize = byteSize;
    }

    public String getResourceName() {
        return resourceName;
    }

    public String getResourceRights() {
        return resourceRights;
    }

    public void setResourceRights(String resourceRights) {
        this.resourceRights = resourceRights;
    }

    @Override
    public String toString() {
        return "DCATMetadata{" +
                "organizationName='" + organizationName + '\'' +
                ", organizationType='" + organizationType + '\'' +
                ", packageDescription='" + packageDescription + '\'' +
                ", packageName='" + packageName + '\'' +
                ", contactPoint='" + contactPoint + '\'' +
                ", contactName='" + contactName + '\'' +
                ", contactEmail='" + contactEmail + '\'' +
                ", keywords=" + keywords +
                ", publisherURL='" + publisherURL + '\'' +
                ", spatialUri='" + spatialUri + '\'' +
                ", spatialCoverage='" + spatialCoverage + '\'' +
                ", temporalStart='" + temporalStart + '\'' +
                ", temporalEnd='" + temporalEnd + '\'' +
                ", themes='" + themes + '\'' +
                ", version='" + version + '\'' +
                ", landingPage='" + landingPage + '\'' +
                ", visibility='" + visibility + '\'' +
                ", datasetRights='" + datasetRights + '\'' +
                ", accessURL='" + accessURL + '\'' +
                ", availability='" + availability + '\'' +
                ", resourceDescription='" + resourceDescription + '\'' +
                ", format='" + format + '\'' +
                ", mimeType='" + mimeType + '\'' +
                ", license='" + license + '\'' +
                ", licenseType='" + licenseType + '\'' +
                ", downloadURL='" + downloadURL + '\'' +
                ", byteSize='" + byteSize + '\'' +
                ", resourceName='" + resourceName + '\'' +
                ", resourceRights='" + resourceRights + '\'' +
                '}';
    }

    public void setResourceName(String resourceName) {
        this.resourceName = resourceName;
    }
}
