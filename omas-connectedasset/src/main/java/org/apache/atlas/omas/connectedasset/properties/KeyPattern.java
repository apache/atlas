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
package org.apache.atlas.omas.connectedasset.properties;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * A KeyPattern defines the type of External Identifier in use of an asset, or the type of Primary Key used within an
 * asset.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public enum KeyPattern implements Serializable
{
    LOCAL_KEY(0, "Local Key", "Unique key allocated and used within the scope of a single system."),
    RECYCLED_KEY(1, "Recycled Key", "Key allocated and used within the scope of a single system that is periodically reused for different records."),
    NATURAL_KEY(2, "Natural Key", "Key derived from an attribute of the entity, such as email address, passport number."),
    MIRROR_KEY(3, "Mirror Key", "Key value copied from another system."),
    AGGREGATE_KEY(4, "Aggregate Key", "Key formed by combining keys from multiple systems."),
    CALLERS_KEY(5, "Caller's Key", "Key from another system can bey used if system name provided."),
    STABLE_KEY(6, "Stable Key", "Key value will remain active even if records are merged."),
    OTHER(99, "Other", "Another key pattern.");

    private static final long     serialVersionUID = 1L;

    private int            keyPatternCode = 99;
    private String         keyPatternName = "";
    private String         keyPatternDescription = "";

    /**
     * Typical Constructor
     */
    KeyPattern(int     keyPatternCode, String   keyPatternName, String   keyPatternDescription)
    {
        /*
         * Save the values supplied
         */
        this.keyPatternCode = keyPatternCode;
        this.keyPatternName = keyPatternName;
        this.keyPatternDescription = keyPatternDescription;
    }


    /**
     * Return the code for this enum instance
     *
     * @return int - key pattern code
     */
    public int getKeyPatternCode()
    {
        return keyPatternCode;
    }


    /**
     * Return the default name for this enum instance.
     *
     * @return String - default name
     */
    public String getKeyPatternName()
    {
        return keyPatternName;
    }


    /**
     * Return the default description for the key pattern for this enum instance.
     *
     * @return String - default description
     */
    public String getKeyPatternDescription()
    {
        return keyPatternDescription;
    }
}