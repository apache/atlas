/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.omrs.metadatacollection.properties.instances;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Relationship is a POJO that manages the properties of an open metadata relationship.  This includes information
 * about the relationship type, the two entities it connects and the properties it holds.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class Relationship extends InstanceHeader
{
    private   InstanceProperties    relationshipProperties = null;

    private   String                entityOnePropertyName  = null;
    private   EntityProxy           entityOneProxy         = null;

    private   String                entityTwoPropertyName  = null;
    private   EntityProxy           entityTwoProxy         = null;


    /**
     * Default constructor - empty relationship linked to nothing.
     */
    public Relationship()
    {
        super();

        /*
         * Nothing else to do - already initialized to null.
         */

    }


    /**
     * Copy/clone constructor - builds a relationship from the supplied template.
     *
     * @param templateElement - template relationship to copy
     */
    public Relationship(Relationship templateElement)
    {
        super(templateElement);

        if (templateElement != null)
        {
            relationshipProperties = templateElement.getProperties();
            entityOnePropertyName  = templateElement.getEntityOnePropertyName();
            entityOneProxy         = templateElement.getEntityOneProxy();
            entityTwoPropertyName  = templateElement.getEntityTwoPropertyName();
            entityTwoProxy         = templateElement.getEntityTwoProxy();
        }
    }


    /**
     * Test to determine if the supplied entity is linked by this relationship.
     *
     * @param entityGUID - unique identifier for the entity to test.
     * @return boolean indicate whether the supplied entity is linked by this relationship
     */
    public boolean relatedToEntity(String  entityGUID)
    {
        if (entityGUID == null)
        {
            return false;
        }

        if (entityOneProxy != null)
        {
            if (entityOneProxy.getGUID().equals(entityGUID))
            {
                return true;
            }
        }

        if (entityTwoProxy != null)
        {
            if (entityTwoProxy.getGUID().equals(entityGUID))
            {
                return true;
            }
        }

        return false;
    }


    /**
     * Return the GUID at the other end of the relationship to the supplied entity.
     *
     * @param entityGUID - unique identifier for the entity to test.
     * @return String guid for the entity at the other end of the relationship.  Null if no matching entity found.
     */
    public String getLinkedEntity(String  entityGUID)
    {
        if ((entityGUID == null) || (entityOneProxy == null) || (entityTwoProxy == null))
        {
            return null;
        }

        String   entityOneGUID = entityOneProxy.getGUID();
        String   entityTwoGUID = entityTwoProxy.getGUID();

        if ((entityOneGUID == null) || entityTwoGUID == null)
        {
            return null;
        }

        if (entityOneGUID.equals(entityGUID))
        {
            return entityTwoGUID;
        }

        if (entityTwoGUID.equals(entityGUID))
        {
            return entityOneGUID;
        }

        return null;
    }


    /**
     * Return an array of the unique identifiers for the entities at either end of the relationship.
     *
     * @return String array
     */
    public String [] getLinkedEntities()
    {
        String[] linkedEntityGUIDs = new String[2];

        if (entityOneProxy == null)
        {
            linkedEntityGUIDs[0] = null;
        }
        else
        {
            linkedEntityGUIDs[0] = entityOneProxy.getGUID();
        }

        if (entityTwoProxy == null)
        {
            linkedEntityGUIDs[1] = null;
        }
        else
        {
            linkedEntityGUIDs[1] = entityTwoProxy.getGUID();
        }

        return linkedEntityGUIDs;
    }


    /**
     * Return a copy of all of the properties for this relationship.  Null means no properties exist.
     *
     * @return InstanceProperties
     */
    public InstanceProperties  getProperties()
    {
        if (relationshipProperties == null)
        {
            return relationshipProperties;
        }
        else
        {
            return new InstanceProperties(relationshipProperties);
        }
    }


    /**
     * Set up the properties for this relationship.
     *
     * @param newProperties - InstanceProperties object
     */
    public void setProperties(InstanceProperties  newProperties)
    {
        relationshipProperties = newProperties;
    }


    /**
     * Return the name of the property for the relationship from the perspective of the entity at the first end of the
     * relationship.
     *
     * @return entityOnePropertyName - String property name
     */
    public String getEntityOnePropertyName() {
        return entityOnePropertyName;
    }


    /**
     * Set up the property name for the relationship from the perspective of the entity at the first end of the
     * relationship.
     *
     * @param entityOnePropertyName - String property name
     */
    public void setEntityOnePropertyName(String entityOnePropertyName)
    {
        this.entityOnePropertyName = entityOnePropertyName;
    }


    /**
     * Return details of the entity at the first end of the relationship.
     *
     * @return entityOneProxy - EntityProxy object for the first end of the relationship.
     */
    public EntityProxy getEntityOneProxy()
    {
        if (entityOneProxy == null)
        {
            return entityOneProxy;
        }
        else
        {
            return new EntityProxy(entityOneProxy);
        }
    }


    /**
     * Set up details of the entity at the first end of the relationship.
     *
     * @param entityOneProxy - EntityProxy object for the first end of the relationship.
     */
    public void setEntityOneProxy(EntityProxy entityOneProxy) { this.entityOneProxy = entityOneProxy; }


    /**
     * Return the property name for the relationship from the perspective of the entity at the second end of the
     * relationship.
     *
     * @return String property name
     */
    public String getEntityTwoPropertyName() { return entityTwoPropertyName; }


    /**
     * Set up the property name for the relationship from the perspective of the entity at the second end of the
     * relationship.
     *
     * @param entityTwoPropertyName - String property name
     */
    public void setEntityTwoPropertyName(String entityTwoPropertyName) { this.entityTwoPropertyName = entityTwoPropertyName; }


    /**
     * Return details of the entity at second end of the relationship.
     *
     * @return EntityProxy object for the second end of the relationship
     */
    public EntityProxy getEntityTwoProxy()
    {
        if (entityTwoProxy == null)
        {
            return entityTwoProxy;
        }
        else
        {
            return new EntityProxy(entityTwoProxy);
        }
    }


    /**
     * Set up the identity of the proxy at the other end of the relationship.
     *
     * @param entityTwoProxy - EntityProxy
     */
    public void setEntityTwoProxy(EntityProxy entityTwoProxy) { this.entityTwoProxy = entityTwoProxy; }


    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "Relationship{" +
                "relationshipProperties=" + relationshipProperties +
                ", entityOneLabel='" + entityOnePropertyName + '\'' +
                ", entityOneProxy=" + entityOneProxy +
                ", entityTwoLabel='" + entityTwoPropertyName + '\'' +
                ", entityTwoProxy=" + entityTwoProxy +
                ", properties=" + getProperties() +
                ", type=" + getType() +
                ", instanceProvenanceType=" + getInstanceProvenanceType() +
                ", metadataCollectionId='" + getMetadataCollectionId() + '\'' +
                ", instanceURL='" + getInstanceURL() + '\'' +
                ", GUID='" + getGUID() + '\'' +
                ", status=" + getStatus() +
                ", createdBy='" + getCreatedBy() + '\'' +
                ", updatedBy='" + getUpdatedBy() + '\'' +
                ", createTime=" + getCreateTime() +
                ", updateTime=" + getUpdateTime() +
                ", version=" + getVersion() +
                ", statusOnDelete=" + getStatusOnDelete() +
                '}';
    }
}
