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
package org.apache.atlas.omrs.admin.properties;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * OpenMetadataExchangeRule controls the sending/receiving of metadata instances on the metadata highway.
 * <ul>
 *     <li>
 *         REGISTRATION_ONLY means do not send/receive reference metadata - just perform the minimal registration
 *         exchanges.
 *     </li>
 *     <li>
 *         JUST_TYPEDEFS - means only send/receive/validate type definitions (TypeDefs).
 *     </li>
 *     <li>
 *         SELECTED_TYPES means that in addition to TypeDefs events, only metadata instances of the types
 *         supplied in the related list of TypeDefs (see typesToSend, typesToSave and typesToFederate) should be processed.
 *     </li>
 *     <li>
 *         LEARNED_TYPES means that the local repository requests reference copies of metadata based on the requests of
 *         the local user community.
 *     </li>
 *     <li>
 *         ALL means send/receive all types of metadata that are supported by the local repository.
 *     </li>
 * </ul>
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public enum OpenMetadataExchangeRule
{
    REGISTRATION_ONLY (0,  "Registration Only", "Only registration exchange; no TypeDefs or metadata instances."),
    JUST_TYPEDEFS     (1,  "Just TypeDefs",     "Only registration and type definitions (TypeDefs) exchange."),
    SELECTED_TYPES    (2,  "Selected Types",    "Registration plus all type definitions (TypeDefs) and metadata " +
                                                "instances (Entities and Relationships) of selected types."),
    LEARNED_TYPES     (3,  "Learned Types",     "Registration plus all type definitions (TypeDefs) and metadata " +
                                                "instances (Entities and Relationships) of types " +
                                                "requested by local users to this server."),
    ALL               (99, "All",               "Registration plus all type definitions (TypeDefs) and metadata " +
                                                "instances (Entities and Relationships).");

    private  int     replicationRuleCode;
    private  String  replicationRuleName;
    private  String  replicationRuleDescription;

    /**
     * Constructor for the metadata instance replication rule.
     *
     * @param replicationRuleCode - the code number of this metadata instance replication rule.
     * @param replicationRuleName - the name of this metadata instance replication rule.
     * @param replicationRuleDescription - the description of this metadata instance replication rule.
     */
    OpenMetadataExchangeRule(int replicationRuleCode, String replicationRuleName, String replicationRuleDescription)
    {
        this.replicationRuleCode = replicationRuleCode;
        this.replicationRuleName = replicationRuleName;
        this.replicationRuleDescription = replicationRuleDescription;
    }


    /**
     * Return the code number of this metadata instance replication rule.
     *
     * @return int replication rule code number
     */
    public int getReplicationRuleCode()
    {
        return replicationRuleCode;
    }


    /**
     * Return the name of this metadata instance replication rule.
     *
     * @return String replication rule name
     */
    public String getReplicationRuleName()
    {
        return replicationRuleName;
    }


    /**
     * Return the description of this metadata instance replication rule.
     *
     * @return String replication rule description
     */
    public String getReplicationRuleDescription()
    {
        return replicationRuleDescription;
    }
}
