package org.apache.nifi.processors.ngsi.ngsi.utils;

import java.util.ArrayList;

public class AttributesLD {
    public String attrName;
    public String attrType;
    public Object attrValue;
    public String datasetId;
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

    public AttributesLD(String attrName, String attrType, String datasetId, Object attrValue,
            boolean hasSubAttrs, ArrayList<AttributesLD> subAttrs) {
        this.attrName = attrName;
        this.attrType = attrType;
        this.datasetId = datasetId;
        this.attrValue = attrValue;
        this.hasSubAttrs = hasSubAttrs;
        this.subAttrs= subAttrs;

    }
}
