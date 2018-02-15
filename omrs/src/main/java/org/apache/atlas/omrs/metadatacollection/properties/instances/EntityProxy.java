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

/**
 * EntityProxy summarizes an entity instance.  It is used to describe one of the entities connected together by a
 * relationship.
 */
public class EntityProxy extends EntitySummary
{
    private InstanceProperties  uniqueAttributes = null;


    /**
     * Default constructor - sets up an empty entity proxy.
     */
    public  EntityProxy()
    {
        super();
    }


    /**
     * Copy/clone constructor for the entity proxy.
     *
     * @param template - entity proxy to copy
     */
    public EntityProxy(EntityProxy   template)
    {
        super(template);

        if (template == null)
        {
            this.uniqueAttributes = template.getUniqueAttributes();
        }
    }


    /**
     * Return a copy of the unique attributes for the entity.
     *
     * @return InstanceProperties iterator
     */
    public InstanceProperties getUniqueAttributes()
    {
        if (uniqueAttributes == null)
        {
            return uniqueAttributes;
        }
        else
        {
            return new InstanceProperties(uniqueAttributes);
        }
    }


    /**
     * Set up the list of unique properties for this entity proxy. These attributes provide properties such
     * as unique names etc that are useful to display.
     *
     * @param uniqueAttributes - InstanceProperties iterator
     */
    public void setUniqueAttributes(InstanceProperties uniqueAttributes) { this.uniqueAttributes = uniqueAttributes; }



    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "EntityProxy{" +
                "uniqueAttributes=" + uniqueAttributes +
                ", classifications=" + getClassifications() +
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
                ", versionName=" + getVersion() +
                ", statusOnDelete=" + getStatusOnDelete() +
                '}';
    }
}
