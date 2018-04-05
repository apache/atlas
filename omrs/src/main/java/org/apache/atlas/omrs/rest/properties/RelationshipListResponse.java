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
import org.apache.atlas.omrs.metadatacollection.properties.instances.Relationship;

import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * RelationshipListResponse holds the response information for an OMRS REST API call that returns a list of
 * relationship objects or an exception.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class RelationshipListResponse extends OMRSRESTAPIPagedResponse
{
    private ArrayList<Relationship>   relationships = null;


    /**
     * Default constructor
     */
    public RelationshipListResponse()
    {
    }


    /**
     * Return the list of relationships for this response.
     *
     * @return list of relationship objects
     */
    public List<Relationship> getRelationships()
    {
        return relationships;
    }


    /**
     * Set up the list of relationships for this response.
     *
     * @param relationships - list of relationship objects
     */
    public void setRelationships(List<Relationship> relationships)
    {
        this.relationships = new ArrayList<>(relationships);
    }


    @Override
    public String toString()
    {
        return "RelationshipListResponse{" +
                "relationships=" + relationships +
                ", nextPageURL='" + nextPageURL + '\'' +
                ", offset=" + offset +
                ", pageSize=" + pageSize +
                ", relatedHTTPCode=" + relatedHTTPCode +
                ", exceptionClassName='" + exceptionClassName + '\'' +
                ", exceptionErrorMessage='" + exceptionErrorMessage + '\'' +
                ", exceptionSystemAction='" + exceptionSystemAction + '\'' +
                ", exceptionUserAction='" + exceptionUserAction + '\'' +
                '}';
    }
}
