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
 * SequencingOrder is used for search requests against a metadata collection.  It defines how the results should
 * to be ordered before they are returned.  This is particularly important when the results are to returned
 * over multiple pages since the caller does not have all of the results at once to perform the sort themselves.
 *
 * The sequencing order values are:
 * <ul>
 *     <li>
 *         ANY - return the results in any order.  This is default.
 *     </li>
 *     <li>
 *         GUID - return in GUID sequence.  This is used when the caller just needs a consistent order in the
 *         order that results are returned.
 *     </li>
 *     <li>
 *         CREATION_DATE_RECENT - return in the order that the elements were created - the most recent ones first.
 *     </li>
 *     <li>
 *         CREATION_DATE_OLDEST - return in the order that the elements were created - the oldest ones first.
 *     </li>
 *     <li>
 *         LAST_UPDATE_RECENT - return in the order of the latest update - the most recent first.
 *     </li>
 *     <li>
 *         LAST_UPDATE_OLDEST - return in the order of the latest update - the oldest first.
 *     </li>
 *     <li>
 *         PROPERTY_ASCENDING - return in ascending order of the values in a sequencing property.  The sequencing
 *         property will be supplied as a parameter.
 *     </li>
 *     <li>
 *         PROPERTY_DESCENDING - return in descending order of the values in a sequencing property.  The sequencing
 *         property will be supplied as a parameter.
 *     </li>
 * </ul>
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public enum SequencingOrder implements Serializable
{
    ANY                  (0, "Any Order",                       "Any order."),
    GUID                 (1, "GUID",                            "Order by GUID."),
    CREATION_DATE_RECENT (2, "Creation Date (Recent First)",    "Order by creation date, most recently created first."),
    CREATION_DATE_OLDEST (3, "Creation Date (Oldest First)",    "Order by creation date, oldest first."),
    LAST_UPDATE_RECENT   (4, "Last Update Date (Recent First)", "Order by last update date, most recently updated first."),
    LAST_UPDATE_OLDEST   (5, "Last Update Date (Oldest First)", "Order by last update date, most recently updated last."),
    PROPERTY_ASCENDING   (6, "By property value (Ascending)",   "Order by property value, lowest value first."),
    PROPERTY_DESCENDING  (7, "By property value (Descending)",  "Order by property value, highest first.");

    private static final long serialVersionUID = 1L;

    private int     ordinal;
    private String  name;
    private String  description;

    /**
     * Constructor to set up a single instances of the enum.
     *
     * @param ordinal - numerical representation of the sequencing order
     * @param name - default string name of the sequencing order
     * @param description - default string description of the sequencing order
     */
    SequencingOrder(int  ordinal, String name, String description)
    {
        this.ordinal = ordinal;
        this.name = name;
        this.description = description;
    }

    /**
     * Return the numeric representation of the sequencing order.
     *
     * @return int ordinal
     */
    public int getOrdinal() { return ordinal; }


    /**
     * Return the default name of the sequencing order.
     *
     * @return String name
     */
    public String getName() { return name; }


    /**
     * Return the default description of the sequencing order.
     *
     * @return String description
     */
    public String getDescription() { return description; }
}
