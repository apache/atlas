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
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.AttributeTypeDef;

import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * AttributeTypeDefListResponse provides a simple baen for returning an array of AttributeTypeDefs
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class AttributeTypeDefListResponse extends OMRSRESTAPIResponse
{
    private ArrayList<AttributeTypeDef> attributeTypeDefs = null;


    /**
     * Default constructor
     */
    public AttributeTypeDefListResponse()
    {
    }


    /**
     * Return the list of AttributeTypeDefs.
     *
     * @return a list of AttributeTypeDefs
     */
    public List<AttributeTypeDef> getAttributeTypeDefs()
    {
        return attributeTypeDefs;
    }


    /**
     * Set up the list of AttributeTypeDefs.
     *
     * @param attributeTypeDefs - a list of AttributeTypeDefs
     */
    public void setAttributeTypeDefs(List<AttributeTypeDef> attributeTypeDefs)
    {
        this.attributeTypeDefs = new ArrayList<>(attributeTypeDefs);
    }


    @Override
    public String toString()
    {
        return "AttributeTypeDefListResponse{" +
                "attributeTypeDefs=" + attributeTypeDefs +
                ", relatedHTTPCode=" + relatedHTTPCode +
                ", exceptionClassName='" + exceptionClassName + '\'' +
                ", exceptionErrorMessage='" + exceptionErrorMessage + '\'' +
                ", exceptionSystemAction='" + exceptionSystemAction + '\'' +
                ", exceptionUserAction='" + exceptionUserAction + '\'' +
                '}';
    }
}
