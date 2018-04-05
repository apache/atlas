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

import org.apache.atlas.ocf.properties.beans.Connection;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefSummary;

import java.util.ArrayList;
import java.util.List;

/**
 * LocalRepositoryConfig provides the properties to control the behavior of the metadata repository associated with
 * this server.
 * <ul>
 *     <li>
 *         metadataCollectionId - unique id of local repository's metadata collection.  If this value is set to
 *         null, the server will generate a unique Id.
 *     </li>
 *     <li>
 *         localRepositoryLocalConnection - the connection properties used to create a locally optimized connector
 *         to the local repository for use by this local server's components. If this value is null then the
 *         localRepositoryRemoteConnection is used.
 *     </li>
 *     <li>
 *         localRepositoryRemoteConnection - the connection properties used to create a connector
 *         to the local repository for use by remote servers.
 *     </li>
 *     <li>
 *         eventsToSaveRule - enumeration describing which open metadata repository events should be saved to
 *         the local repository.
 *     </li>
 *     <li>
 *         selectedTypesToSave - list of TypeDefs in supported of the eventsToSave.SELECTED_TYPES option.
 *     </li>
 *     <li>
 *         eventsToSendRule - enumeration describing which open metadata repository events should be sent from
 *         the local repository.
 *     </li>
 *     <li>
 *         selectedTypesToSend - list of TypeDefs in supported of the eventsToSend.SELECTED_TYPES option.
 *     </li>
 *     <li>
 *         eventMapperConnection - the connection properties for the event mapper for the local repository.
 *         The event mapper is an optional component used when the local repository has proprietary external
 *         APIs that can change metadata in the repository without going through the OMRS interfaces.
 *         It maps the proprietary events from the local repository to the OMRS Events.
 *     </li>
 * </ul>
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class LocalRepositoryConfig
{
    private String                    metadataCollectionId              = null;
    private Connection                localRepositoryLocalConnection    = null;
    private Connection                localRepositoryRemoteConnection   = null;
    private OpenMetadataExchangeRule  eventsToSaveRule                  = null;
    private ArrayList<TypeDefSummary> selectedTypesToSave               = null;
    private OpenMetadataExchangeRule  eventsToSendRule                  = null;
    private ArrayList<TypeDefSummary> selectedTypesToSend               = null;
    private Connection                eventMapperConnection             = null;


    /**
     * Constructor
     *
     * @param metadataCollectionId - unique id of local repository's metadata collection
     * @param localRepositoryLocalConnection - the connection properties used to create a locally optimized connector
     *         to the local repository for use by this local server's components.
     * @param localRepositoryRemoteConnection - the connection properties used to create a connector
     *         to the local repository for use by remote servers.
     * @param eventsToSaveRule - enumeration describing which open metadata repository events should be saved to
     *                         the local repository.
     * @param selectedTypesToSave - list of TypeDefs in supported of the eventsToSave.SELECTED_TYPES option.
     * @param eventsToSendRule - enumeration describing which open metadata repository events should be sent from
     *                         the local repository.
     * @param selectedTypesToSend - list of TypeDefs in supported of the eventsToSend.SELECTED_TYPES option.
     * @param eventMapperConnection - Connection for the local repository's event mapper.  This is optional.
     */
    public LocalRepositoryConfig(String                    metadataCollectionId,
                                 Connection                localRepositoryLocalConnection,
                                 Connection                localRepositoryRemoteConnection,
                                 OpenMetadataExchangeRule  eventsToSaveRule,
                                 List<TypeDefSummary>      selectedTypesToSave,
                                 OpenMetadataExchangeRule  eventsToSendRule,
                                 List<TypeDefSummary>      selectedTypesToSend,
                                 Connection                eventMapperConnection)
    {
        this.metadataCollectionId = metadataCollectionId;
        this.localRepositoryLocalConnection = localRepositoryLocalConnection;
        this.localRepositoryRemoteConnection = localRepositoryRemoteConnection;
        this.eventsToSaveRule = eventsToSaveRule;
        this.setSelectedTypesToSave(selectedTypesToSave);
        this.eventsToSendRule = eventsToSendRule;
        this.setSelectedTypesToSend(selectedTypesToSend);
        this.eventMapperConnection = eventMapperConnection;
    }


    /**
     * Default constructor used for JSON to Java processes - does not do anything useful because all
     * local variables are initialized to null in their declaration.
     */
    public LocalRepositoryConfig()
    {
    }


    /**
     * Return the unique id of local repository's metadata collection.  If this value is set to
     * null, the server will generate a unique Id.
     *
     * @return String unique Id
     */
    public String getMetadataCollectionId()
    {
        return metadataCollectionId;
    }


    /**
     * Set up the unique id of local repository's metadata collection.  If this value is set to
     * null, the server will generate a unique Id.
     *
     * @param metadataCollectionId - String unique Id
     */
    public void setMetadataCollectionId(String metadataCollectionId)
    {
        this.metadataCollectionId = metadataCollectionId;
    }


    /**
     * Return the connection properties used to create a locally optimized connector to the local repository for
     * use by this local server's components.  If this value is null then the localRepositoryRemoteConnection is used.
     *
     * @return Connection properties object
     */
    public Connection getLocalRepositoryLocalConnection()
    {
        return localRepositoryLocalConnection;
    }


    /**
     * Set up the connection properties used to create a locally optimized connector to the local repository for
     * use by this local server's components.  If this value is null then the localRepositoryRemoteConnection is used.
     *
     * @param localRepositoryLocalConnection - Connection properties object
     */
    public void setLocalRepositoryLocalConnection(Connection localRepositoryLocalConnection)
    {
        this.localRepositoryLocalConnection = localRepositoryLocalConnection;
    }


    /**
     * Return the connection properties used to create a connector to the local repository for use by remote servers.
     *
     * @return Connection properties object
     */
    public Connection getLocalRepositoryRemoteConnection()
    {
        return localRepositoryRemoteConnection;
    }


    /**
     * Set up the connection properties used to create a connector to the local repository for use by remote servers.
     *
     * @param localRepositoryRemoteConnection - Connection properties object
     */
    public void setLocalRepositoryRemoteConnection(Connection localRepositoryRemoteConnection)
    {
        this.localRepositoryRemoteConnection = localRepositoryRemoteConnection;
    }


    /**
     * Return the enumeration describing which open metadata repository events should be saved to
     * the local repository.
     *
     * @return OpenMetadataExchangeRule enum
     */
    public OpenMetadataExchangeRule getEventsToSaveRule()
    {
        return eventsToSaveRule;
    }


    /**
     * Set up the enumeration describing which open metadata repository events should be saved to
     * the local repository.
     *
     * @param eventsToSaveRule - OpenMetadataExchangeRule enum
     */
    public void setEventsToSaveRule(OpenMetadataExchangeRule eventsToSaveRule)
    {
        this.eventsToSaveRule = eventsToSaveRule;
    }


    /**
     * Return the list of TypeDefs in supported of the eventsToSave.SELECTED_TYPES option.
     *
     * @return list of types
     */
    public List<TypeDefSummary> getSelectedTypesToSave()
    {
        if (selectedTypesToSave == null)
        {
            return null;
        }
        else
        {
            return selectedTypesToSave;
        }
    }


    /**
     * Set up the list of TypeDefs in supported of the eventsToSave.SELECTED_TYPES option.
     *
     * @param selectedTypesToSave - list of types
     */
    public void setSelectedTypesToSave(List<TypeDefSummary> selectedTypesToSave)
    {
        if (selectedTypesToSave == null)
        {
            this.selectedTypesToSave = null;
        }
        else
        {
            this.selectedTypesToSave = new ArrayList<>(selectedTypesToSave);
        }
    }


    /**
     * Return the enumeration describing which open metadata repository events should be sent from
     * the local repository.
     *
     * @return OpenMetadataExchangeRule enum
     */
    public OpenMetadataExchangeRule getEventsToSendRule()
    {
        return eventsToSendRule;
    }


    /**
     * Set up the enumeration describing which open metadata repository events should be sent from
     * the local repository.
     *
     * @param eventsToSendRule - OpenMetadataExchangeRule enum
     */
    public void setEventsToSendRule(OpenMetadataExchangeRule eventsToSendRule)
    {
        this.eventsToSendRule = eventsToSendRule;
    }


    /**
     * Return the list of TypeDefs in supported of the eventsToSend.SELECTED_TYPES option.
     *
     * @return list of types
     */
    public List<TypeDefSummary> getSelectedTypesToSend()
    {
        if (selectedTypesToSend == null)
        {
            return null;
        }
        else
        {
            return selectedTypesToSend;
        }
    }


    /**
     * Set up the list of TypeDefs in supported of the eventsToSend.SELECTED_TYPES option.
     *
     * @param selectedTypesToSend - list of types
     */
    public void setSelectedTypesToSend(List<TypeDefSummary> selectedTypesToSend)
    {
        if (selectedTypesToSend == null)
        {
            this.selectedTypesToSend = null;
        }
        else
        {
            this.selectedTypesToSend = new ArrayList<>(selectedTypesToSend);
        }
    }


    /**
     * Return the connection properties for the event mapper for the local repository.  The event mapper is an
     * optional component used when the local repository has proprietary external APIs that can change metadata
     * in the repository without going through the OMRS interfaces.  It maps the proprietary events from
     * the local repository to the OMRS Events.
     *
     * @return Connection properties object
     */
    public Connection getEventMapperConnection()
    {
        return eventMapperConnection;
    }


    /**
     * Set up the connection properties for the event mapper for the local repository.  The event mapper is an
     * optional component used when the local repository has proprietary external APIs that can change metadata
     * in the repository without going through the OMRS interfaces.  It maps the proprietary events from
     * the local repository to the OMRS Events.
     *
     * @param eventMapperConnection - Connection properties object
     */
    public void setEventMapperConnection(Connection eventMapperConnection)
    {
        this.eventMapperConnection = eventMapperConnection;
    }
}
