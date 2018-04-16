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
package org.apache.atlas.omas.assetconsumer.listener;

import org.apache.atlas.ocf.properties.Connection;
import org.apache.atlas.omas.assetconsumer.publisher.AssetConsumerPublisher;
import org.apache.atlas.omrs.eventmanagement.events.OMRSEventOriginator;
import org.apache.atlas.omrs.eventmanagement.events.OMRSInstanceEvent;
import org.apache.atlas.omrs.eventmanagement.events.OMRSInstanceEventType;
import org.apache.atlas.omrs.eventmanagement.events.v1.OMRSEventV1;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryHelper;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryValidator;
import org.apache.atlas.omrs.topicconnectors.OMRSTopicListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssetConsumerOMRSTopicListener implements OMRSTopicListener
{
    private static final Logger log = LoggerFactory.getLogger(AssetConsumerOMRSTopicListener.class);

    private AssetConsumerPublisher   publisher;


    /**
     * The constructor is given the connection to the out topic for Asset Consumer OMAS
     * along with classes for testing and manipulating instances.
     *
     * @param assetConsumerOutTopic - connection to the out topic
     * @param repositoryHelper - provides methods for working with metadata instances
     * @param repositoryValidator - provides validation of metadata instance
     * @param componentName - name of component
     */
    public AssetConsumerOMRSTopicListener(Connection              assetConsumerOutTopic,
                                          OMRSRepositoryHelper    repositoryHelper,
                                          OMRSRepositoryValidator repositoryValidator,
                                          String                  componentName)
    {
        publisher = new AssetConsumerPublisher(assetConsumerOutTopic,
                                               repositoryHelper,
                                               repositoryValidator,
                                               componentName);
    }


    /**
     * Method to pass an event received on topic.
     *
     * @param event - inbound event
     */
    public void processEvent(OMRSEventV1 event)
    {
        /*
         * The event should not be null but worth checking.
         */
        if (event != null)
        {
            /*
             * Determine the category of event to process.
             */
            switch (event.getEventCategory())
            {
                case REGISTRY:
                    if (log.isDebugEnabled())
                    {
                        log.debug("Ignoring registry event: " + event.toString());
                    }
                    break;

                case TYPEDEF:
                    if (log.isDebugEnabled())
                    {
                        log.debug("Ignoring type event: " + event.toString());
                    }
                    break;

                case INSTANCE:
                    this.processInstanceEvent(new OMRSInstanceEvent(event));
                    break;

                default:
                    if (log.isDebugEnabled())
                    {
                        log.debug("Unknown event received :|");
                    }
            }
        }
        else
        {
            if (log.isDebugEnabled())
            {
                log.debug("Null OMRS Event received :(");
            }
        }
    }


    /**
     * Unpack and deliver an instance event to the InstanceEventProcessor
     *
     * @param instanceEvent - event to unpack
     */
    private void processInstanceEvent(OMRSInstanceEvent  instanceEvent)
    {
        if (log.isDebugEnabled())
        {
            log.debug("Processing instance event", instanceEvent);
        }

        if (instanceEvent == null)
        {
            if (log.isDebugEnabled())
            {
                log.debug("Null instance event - ignoring event");
            }
        }
        else
        {
            OMRSInstanceEventType instanceEventType       = instanceEvent.getInstanceEventType();
            OMRSEventOriginator   instanceEventOriginator = instanceEvent.getEventOriginator();

            if ((instanceEventType != null) && (instanceEventOriginator != null))
            {
                switch (instanceEventType)
                {
                    case NEW_ENTITY_EVENT:
                        publisher.processNewEntity(instanceEvent.getEntity());
                        break;

                    case UPDATED_ENTITY_EVENT:
                        publisher.processUpdatedEntity(instanceEvent.getEntity());
                        break;

                    case CLASSIFIED_ENTITY_EVENT:
                        publisher.processUpdatedEntity(instanceEvent.getEntity());
                        break;

                    case RECLASSIFIED_ENTITY_EVENT:
                        publisher.processUpdatedEntity(instanceEvent.getEntity());
                        break;

                    case DECLASSIFIED_ENTITY_EVENT:
                        publisher.processUpdatedEntity(instanceEvent.getEntity());
                        break;

                    case DELETED_ENTITY_EVENT:
                        publisher.processDeletedEntity(instanceEvent.getEntity());
                        break;

                    case PURGED_ENTITY_EVENT:
                        if (log.isDebugEnabled())
                        {
                            log.debug("Ignoring entity purge events");
                        }
                        break;

                    case UNDONE_ENTITY_EVENT:
                        publisher.processUpdatedEntity(instanceEvent.getEntity());
                        break;

                    case RESTORED_ENTITY_EVENT:
                        publisher.processRestoredEntity(instanceEvent.getEntity());
                        break;

                    case REFRESH_ENTITY_REQUEST:
                    case REFRESHED_ENTITY_EVENT:
                    case RE_HOMED_ENTITY_EVENT:
                    case RETYPED_ENTITY_EVENT:
                    case RE_IDENTIFIED_ENTITY_EVENT:
                        if (log.isDebugEnabled())
                        {
                            log.debug("Ignoring entity repository maintenance events");
                        }
                        break;

                    case NEW_RELATIONSHIP_EVENT:
                        publisher.processNewRelationship(instanceEvent.getRelationship());
                        break;

                    case UPDATED_RELATIONSHIP_EVENT:
                        publisher.processUpdatedRelationship(instanceEvent.getRelationship());
                        break;

                    case UNDONE_RELATIONSHIP_EVENT:
                        publisher.processUpdatedRelationship(instanceEvent.getRelationship());
                        break;

                    case DELETED_RELATIONSHIP_EVENT:
                        publisher.processDeletedRelationship(instanceEvent.getRelationship());

                        break;

                    case PURGED_RELATIONSHIP_EVENT:
                        if (log.isDebugEnabled())
                        {
                            log.debug("Ignoring relationship purge events");
                        }
                        break;

                    case RESTORED_RELATIONSHIP_EVENT:
                        publisher.processRestoredRelationship(instanceEvent.getRelationship());

                        break;

                    case REFRESH_RELATIONSHIP_REQUEST:
                    case REFRESHED_RELATIONSHIP_EVENT:
                    case RE_IDENTIFIED_RELATIONSHIP_EVENT:
                    case RE_HOMED_RELATIONSHIP_EVENT:
                    case RETYPED_RELATIONSHIP_EVENT:

                        if (log.isDebugEnabled())
                        {
                            log.debug("Ignoring relationship repository maintenance events");
                        }
                        break;

                    case INSTANCE_ERROR_EVENT:

                        if (log.isDebugEnabled())
                        {
                            log.debug("Ignoring instance error events");
                        }
                        break;
                }
            }
            else
            {
                if (log.isDebugEnabled())
                {
                    log.debug("Ignored instance event - null type");
                }
            }
        }
    }
}
