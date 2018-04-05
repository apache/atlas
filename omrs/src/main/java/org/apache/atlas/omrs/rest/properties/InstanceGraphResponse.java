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
package org.apache.atlas.omrs.rest.properties;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.atlas.omrs.metadatacollection.properties.instances.EntityDetail;
import org.apache.atlas.omrs.metadatacollection.properties.instances.InstanceGraph;
import org.apache.atlas.omrs.metadatacollection.properties.instances.Relationship;

import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * InstanceGraphResponse is the response structure for an OMRS REST API call that returns an instance graph object.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class InstanceGraphResponse extends OMRSRESTAPIResponse
{
    private ArrayList<EntityDetail> entityElementList       = null;
    private ArrayList<Relationship> relationshipElementList = null;


    /**
     * Default constructor
     */
    public InstanceGraphResponse()
    {
    }

    public List<EntityDetail> getEntityElementList()
    {
        return entityElementList;
    }

    public void setEntityElementList(List<EntityDetail> entityElementList)
    {
        this.entityElementList = new ArrayList<>(entityElementList);
    }


    /**
     * Return the list of relationships that are part of this instance graph.
     *
     * @return - list of relationships
     */
    public List<Relationship> getRelationshipElementList()
    {
        return relationshipElementList;
    }


    /**
     * Set up the list of relationships that are part of this instance graph.
     *
     * @param relationshipElementList - list of relationships
     */
    public void setRelationshipElementList(List<Relationship> relationshipElementList)
    {
        this.relationshipElementList = new ArrayList<>(relationshipElementList);
    }
}
