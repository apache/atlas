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
package org.apache.atlas.omrs.metadatacollection.properties;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * The MatchCriteria enum defines how the metadata instances in the metadata collection should be matched
 * against the properties supplied on the search request.
 * <ul>
 *     <li>ALL means all properties must match.</li>
 *     <li>ANY means a match on any of properties is good enough.</li>
 *     <li>NONE means return instances where none of the supplied properties match.</li>
 * </ul>
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public enum MatchCriteria implements Serializable
{
    ALL  (0, "All",  "All properties must match."),
    ANY  (1, "Any",  "A match on any of properties in the instance is good enough."),
    NONE (2, "None", "Return instances where none of the supplied properties match.");

    private static final long serialVersionUID = 1L;

    private int     ordinal;
    private String  name;
    private String  description;

    /**
     * Constructor to set up a single instances of the enum.
     *
     * @param ordinal - numerical representation of the match criteria
     * @param name - default string name of the match criteria
     * @param description - default string description of the match criteria
     */
    MatchCriteria(int  ordinal, String name, String description)
    {
        this.ordinal = ordinal;
        this.name = name;
        this.description = description;
    }

    /**
     * Return the numeric representation of the match criteria.
     *
     * @return int ordinal
     */
    public int getOrdinal() { return ordinal; }


    /**
     * Return the default name of the match criteria.
     *
     * @return String name
     */
    public String getName() { return name; }


    /**
     * Return the default description of the match criteria.
     *
     * @return String description
     */
    public String getDescription() { return description; }
}
