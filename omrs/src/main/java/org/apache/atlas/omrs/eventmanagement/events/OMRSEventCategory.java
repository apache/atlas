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
package org.apache.atlas.omrs.eventmanagement.events;


/**
 * OMRSEventCategory defines the different categories of events that pass through the OMRS Topic.
 * <ul>
 *     <li>
 *         UNKNOWN - this is either an uninitialized event, or the incoming event is not supported by the
 *         local server.
 *     </li>
 *     <li>
 *         REGISTRY - this is an event used by the cohort registries to manage the membership
 *         of the cohort.
 *     </li>
 *     <li>
 *         TYPEDEF - this is an event used by the metadata repository connectors to synchronize the metadata types
 *         (stored in TypeDefs) across the metadata repository cohort.
 *     </li>
 * </ul>
 */
public enum OMRSEventCategory
{
    UNKNOWN (0, "Unknown Event",  "Unknown event category"),
    REGISTRY(1, "Registry Event", "Event used to manage the membership of the metadata repository cohort"),
    TYPEDEF (2, "TypeDef Event",  "Event used to manage the synchronization of TypeDefs within the metadata repository cohort"),
    INSTANCE(3, "Instance Event", "Event used to manage the replication of metadata instances within the metadata repository cohort"),
    GENERIC (99, "Generic Event", "Event used for sending generic messages - typically error messages.");


    private int    categoryCode;
    private String categoryName;
    private String categoryDescription;


    /**
     * Default constructor.
     *
     * @param categoryCode - int category code number
     * @param categoryName - String category name
     * @param categoryDescription - String category description
     */
    OMRSEventCategory(int      categoryCode,
                      String   categoryName,
                      String   categoryDescription)
    {
        this.categoryCode = categoryCode;
        this.categoryName = categoryName;
        this.categoryDescription = categoryDescription;
    }


    /**
     * Return the code number for the event category.
     *
     * @return int code number
     */
    public int getEventCategoryCode()
    {
        return categoryCode;
    }


    /**
     * Return the name of the event category.
     *
     * @return String name
     */
    public String getEventCategoryName()
    {
        return categoryName;
    }


    /**
     * Return the default description of the event category.  This description is in English and is a default
     * value for the situation when the natural language resource bundle for Event Category is not available.
     *
     * @return String default description
     */
    public String getEventCategoryDescription()
    {
        return categoryDescription;
    }
}
