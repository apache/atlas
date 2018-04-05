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
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDef;

import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * TypeDefListResponse provides a simple bean for returning a list of TypeDefs (or information to create
 * a valid OMRS exception).
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class TypeDefListResponse extends OMRSRESTAPIResponse
{
    private ArrayList<TypeDef> typeDefs = null;

    /**
     * Default Constructor
     */
    public TypeDefListResponse()
    {
    }


    /**
     * Return the list of typeDefs
     *
     * @return - list of typeDefs
     */
    public List<TypeDef> getTypeDefs()
    {
        return typeDefs;
    }


    /**
     * Set up the list of typeDefs
     *
     * @param typeDefs - list of typeDefs
     */
    public void setTypeDefs(List<TypeDef> typeDefs)
    {
        this.typeDefs = new ArrayList<>(typeDefs);
    }


    @Override
    public String toString()
    {
        return "TypeDefListResponse{" +
                "typeDefs=" + typeDefs +
                ", relatedHTTPCode=" + relatedHTTPCode +
                ", exceptionClassName='" + exceptionClassName + '\'' +
                ", exceptionErrorMessage='" + exceptionErrorMessage + '\'' +
                ", exceptionSystemAction='" + exceptionSystemAction + '\'' +
                ", exceptionUserAction='" + exceptionUserAction + '\'' +
                '}';
    }
}
