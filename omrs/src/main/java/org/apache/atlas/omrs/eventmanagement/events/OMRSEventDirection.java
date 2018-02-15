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
 * OMRSEventDirection defines the origin of an OMRSEvent.  It is used primarily for logging and debug.
 */
public enum OMRSEventDirection
{
    UNKNOWN  (0, "<Unknown>     ", "Uninitialized event direction"),
    INBOUND  (1, "Inbound Event ", "Event from a remote member of the open metadata repository cluster."),
    OUTBOUND (2, "Outbound Event", "Event from local server to other members of the open metadata repository cluster.");


    private  int    eventDirectionCode;
    private  String eventDirectionName;
    private  String eventDirectionDescription;


    /**
     * Default constructor - sets up the specific values for this enum instance.
     *
     * @param eventDirectionCode - int identifier for the enum, used for indexing arrays etc with the enum.
     * @param eventDirectionName - String name for the enum, used for message content.
     * @param eventDirectionDescription - String default description for the enum, used when there is not natural
     *                             language resource bundle available.
     */
    OMRSEventDirection(int eventDirectionCode, String eventDirectionName, String eventDirectionDescription)
    {
        this.eventDirectionCode = eventDirectionCode;
        this.eventDirectionName = eventDirectionName;
        this.eventDirectionDescription = eventDirectionDescription;
    }


    /**
     * Return the identifier for the enum, used for indexing arrays etc with the enum.
     *
     * @return int identifier
     */
    public int getEventDirectionCode()
    {
        return eventDirectionCode;
    }


    /**
     * Return the name for the enum, used for message content.
     *
     * @return String name
     */
    public String getEventDirectionName()
    {
        return eventDirectionName;
    }


    /**
     * Return the default description for the enum, used when there is not natural
     * language resource bundle available.
     *
     * @return String default description
     */
    public String getEventDirectionDescription()
    {
        return eventDirectionDescription;
    }
}
