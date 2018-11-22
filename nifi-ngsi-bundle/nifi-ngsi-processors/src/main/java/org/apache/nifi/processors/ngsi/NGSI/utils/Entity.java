package org.apache.nifi.processors.ngsi.NGSI.utils;

import java.util.ArrayList;

public class Entity {
    public String entityId;
    public String entityType;
    public ArrayList<Attributes> entityAttrs;

    public Entity(String entityId, String entityType, ArrayList<Attributes> entityAttrs) {
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

    public ArrayList<Attributes> getEntityAttrs() {
        return entityAttrs;
    }

    public void setEntityAttrs(ArrayList<Attributes> entityAttrs) {
        this.entityAttrs = entityAttrs;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }
}
