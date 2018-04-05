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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * The InstanceStatus defines the status of a relationship or an entity in the metadata collection.  It effectively
 * defines its visibility to different types of queries.  Most queries by default will only return instances in the
 * active status.
 * <ul>
 *     <li>UNKNOWN - the instance has not been initialized.</li>
 *     <li>PROPOSED - the instance has not yet been stored in the metadata collection.</li>
 *     <li>DRAFT - the instance is stored but not fully filled out so should not be used for normal queries.</li>
 *     <li>PREPARED - the instance is stored and complete - it is ready to be moved to active status.</li>
 *     <li>ACTIVE - the instance is in active use.</li>
 *     <li>DELETED - the instance has been deleted and is waiting to be purged.  It is kept in the metadata collection
 *     to support a restore request.  It is not returned on normal queries.</li>
 * </ul>
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public enum InstanceStatus implements Serializable
{
    UNKNOWN (0, "<Unknown>", "Unknown instance status."),
    PROPOSED(1, "Proposed",  "Proposed instance to store in the metadata collection."),
    DRAFT   (2, "Draft",     "Potentially incomplete draft of instance still being developed."),
    PREPARED(3, "Prepared",  "Complete draft of instance waiting for approval."),
    ACTIVE (10, "Active",    "Active instance in use."),
    DELETED(99, "Deleted",   "Instance that has been deleted and is no longer in use.");

    private static final long serialVersionUID = 1L;

    private  int     ordinal;
    private  String  statusName;
    private  String  statusDescription;


    /**
     * Default constructor sets up the specific values for an enum instance.
     *
     * @param ordinal - int enum value ordinal
     * @param statusName - String name
     * @param statusDescription - String description
     */
    InstanceStatus(int     ordinal,
                   String  statusName,
                   String  statusDescription)
    {
        this.ordinal = ordinal;
        this.statusName = statusName;
        this.statusDescription = statusDescription;
    }


    /**
     * Return the numerical value for the enum.
     *
     * @return int enum value ordinal
     */
    public int getOrdinal() { return ordinal; }


    /**
     * Return the descriptive name for the enum.
     *
     * @return String name
     */
    public String getStatusName() { return statusName; }


    /**
     * Return the description for the enum.
     *
     * @return String description
     */
    public String getStatusDescription() { return statusDescription; }
}
