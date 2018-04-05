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
package org.apache.atlas.omrs.metadatacollection.properties.typedefs;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * The RelationshipContainerEnd enum defines which end of the relationship is the container (where the diamond is
 * in UML-speak).  NOT_APPLICABLE is used on an association.  END1 or END2 is used on an aggregation or composition.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public enum RelationshipContainerEnd implements Serializable
{
    NOT_APPLICABLE  (0, "Not Applicable",  "This relationship does not support containment."),
    END1            (1, "End 1",           "The containment is at end 1."),
    END2            (2, "End 2",           "The containment is at end 2.");

    private static final long serialVersionUID = 1L;

    private int     ordinal;
    private String  name;
    private String  description;

    /**
     * Constructor to set up a single instances of the enum.
     *
     * @param ordinal - numerical representation of the container end
     * @param name - default string name of the container end
     * @param description - default string description of the container
     */
    RelationshipContainerEnd(int  ordinal, String name, String description)
    {
        this.ordinal = ordinal;
        this.name = name;
        this.description = description;
    }

    /**
     * Return the numeric representation of the container end indicator.
     *
     * @return int ordinal
     */
    public int getOrdinal() { return ordinal; }


    /**
     * Return the default name of the container end indicator.
     *
     * @return String name
     */
    public String getName() { return name; }


    /**
     * Return the default description of the container end indicator.
     *
     * @return String description
     */
    public String getDescription() { return description; }
}
