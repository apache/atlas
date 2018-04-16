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
package org.apache.atlas.ocf.properties.beans;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.HashMap;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * The AdditionalProperties bean extends the AdditionalProperties from the properties package with a default constructor and
 * setter methods.  This means it can be used for REST calls and other JSON based functions.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class AdditionalProperties extends org.apache.atlas.ocf.properties.AdditionalProperties
{

    /**
     * Default constructor
     */
    public AdditionalProperties()
    {
        super(null);
    }


    /**
     * Set up the additional properties.
     *
     * @param additionalProperties - property map
     */
    public void setAdditionalProperties(Map<String, Object> additionalProperties)
    {
        super.additionalProperties = additionalProperties;
    }


    /**
     * Copy/clone Constructor for additional properties that are connected to an asset.
     *
     * @param templateProperties - template object to copy.
     */
    public AdditionalProperties(AdditionalProperties templateProperties)
    {
        super(null, templateProperties);
    }
}
