package org.apache.nifi.processors.ngsi.ngsi.utils;

import java.util.ArrayList;

public class AttributesLD {
    public String attrName;
    public String attrType;
    public Object attrValue;
    public String datasetId;

    public String observedAt;

    public String createdAt;

    public String modifiedAt;

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public String getModifiedAt() {
        return modifiedAt;
    }

    public void setModifiedAt(String modifiedAt) {
        this.modifiedAt = modifiedAt;
    }

    public String getObservedAt() {
        return observedAt;
    }

    public void setObservedAt(String observedAt) {
        this.observedAt = observedAt;
    }

    public boolean hasSubAttrs;
    public ArrayList<AttributesLD> subAttrs;

    public boolean isHasSubAttrs() {
        return hasSubAttrs;
    }

    public ArrayList<AttributesLD> getSubAttrs() {
        return subAttrs;
    }

    public String getAttrName() {
        return attrName;
    }

    public void setAttrName(String attrName) {
        this.attrName = attrName;
    }

    public String getAttrType() {
        return attrType;
    }

    public void setAttrType(String attrType) {
        this.attrType = attrType;
    }

    public Object getAttrValue() {
        return attrValue;
    }

    public void setAttrValue(Object attrValue) {
        this.attrValue = attrValue;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public AttributesLD(String attrName, String attrType, String datasetId, String observedAt, Object attrValue,
                        boolean hasSubAttrs, ArrayList<AttributesLD> subAttrs) {
        this.attrName = attrName;
        this.attrType = attrType;
        this.datasetId = datasetId;
        this.observedAt = observedAt;
        this.attrValue = attrValue;
        this.hasSubAttrs = hasSubAttrs;
        this.subAttrs= subAttrs;

    }
}
