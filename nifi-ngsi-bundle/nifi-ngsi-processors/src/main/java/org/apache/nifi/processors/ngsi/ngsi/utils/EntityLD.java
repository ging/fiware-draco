package org.apache.nifi.processors.ngsi.ngsi.utils;

import java.util.ArrayList;

public class EntityLD {
    public String entityId;
    public String entityType;
    public ArrayList<AttributesLD> entityAttrs;

    public EntityLD(String entityId, String entityType, ArrayList<AttributesLD> entityAttrs) {
        this.entityId = entityId;
        this.entityType = entityType;
        this.entityAttrs = entityAttrs;
    }

    public String getEntityType() {
        return entityType;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    public ArrayList<AttributesLD> getEntityAttrs() {
        return entityAttrs;
    }

    public void setEntityAttrs(ArrayList<AttributesLD> entityAttrs) {
        this.entityAttrs = entityAttrs;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }
}
