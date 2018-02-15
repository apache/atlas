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
package org.apache.atlas.omrs.eventmanagement;

import org.apache.atlas.omrs.admin.properties.OpenMetadataExchangeRule;
import org.apache.atlas.omrs.metadatacollection.properties.instances.EntityDetail;
import org.apache.atlas.omrs.metadatacollection.properties.instances.Relationship;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefSummary;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSTypeDefValidator;

import java.util.ArrayList;

/**
 * OMRSRepositoryEventExchangeRule determines if particular types of events should be exchanged on the OMRS Topic.
 */
public class OMRSRepositoryEventExchangeRule
{
    private String                    sourceName;
    private OMRSTypeDefValidator      typeDefValidator;
    private OpenMetadataExchangeRule  exchangeRule;
    private ArrayList<TypeDefSummary> selectedTypesToProcess;


    /**
     * Constructor provides all of the objects used in the event exchange decision.
     *
     * @param sourceName - name of the caller
     * @param typeDefValidator - local manager of the type definitions (TypeDefs) used by the local repository.
     * @param exchangeRule - enum detailing the types of events to process.
     * @param selectedTypesToProcess - supplementary list to support selective processing of events.
     */
    public OMRSRepositoryEventExchangeRule(String                    sourceName,
                                           OMRSTypeDefValidator      typeDefValidator,
                                           OpenMetadataExchangeRule  exchangeRule,
                                           ArrayList<TypeDefSummary> selectedTypesToProcess)
    {
        this.sourceName = sourceName;
        this.typeDefValidator = typeDefValidator;
        this.exchangeRule = exchangeRule;
        this.selectedTypesToProcess = selectedTypesToProcess;
    }


    /**
     * Determine if TypeDef events should be processed.
     *
     * @return boolean flag
     */
    public boolean processTypeDefEvents()
    {
        if (exchangeRule != OpenMetadataExchangeRule.REGISTRATION_ONLY)
        {
            return true;
        }
        else
        {
            return false;
        }
    }


    /**
     * Determine from the type of the instance if an instance event should be processed.
     *
     * @param typeDefGUID - unique identifier of the type
     * @param typeDefName - unique name of the type
     * @return boolean flag
     */
    public boolean processInstanceEvent(String   typeDefGUID, String   typeDefName)
    {
        if (typeDefValidator == null)
        {
            return false;
        }
        else
        {
            return typeDefValidator.isActiveType(sourceName, typeDefGUID, typeDefName);
        }
    }


    /**
     * Determine from the type of the instance if an instance event should be processed.
     *
     * @param typeDefSummary - details of the type
     * @return boolean flag
     */
    public boolean processInstanceEvent(TypeDefSummary   typeDefSummary)
    {
        if (typeDefValidator == null)
        {
            return false;
        }
        if (typeDefSummary == null)
        {
            return false;
        }
        else
        {
            return typeDefValidator.isActiveType(sourceName, typeDefSummary.getGUID(), typeDefSummary.getName());
        }
    }


    /**
     * Determine from the type of the instance if an instance event should be processed.
     *
     * @param entity - details of the instance to test
     * @return boolean flag
     */
    public boolean processInstanceEvent(EntityDetail   entity)
    {
        if (typeDefValidator == null)
        {
            return false;
        }
        else if (entity == null)
        {
            return false;
        }
        else
        {
            // TODO
            return true;
        }
    }


    /**
     * Determine from the type of the instance if an instance event should be processed.
     *
     * @param relationship - details of the instance to test
     * @return boolean flag
     */
    public boolean processInstanceEvent(Relationship relationship)
    {
        if (typeDefValidator == null)
        {
            return false;
        }
        else if (relationship == null)
        {
            return false;
        }
        else
        {
            // TODO
            return true;
        }
    }
}
