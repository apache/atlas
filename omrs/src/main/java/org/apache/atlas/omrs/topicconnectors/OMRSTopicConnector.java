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
package org.apache.atlas.omrs.topicconnectors;

import org.apache.atlas.ocf.ConnectorBase;
import org.apache.atlas.ocf.ffdc.ConnectorCheckedException;
import org.apache.atlas.ocf.properties.Connection;
import org.apache.atlas.ocf.properties.Endpoint;
import org.apache.atlas.omrs.auditlog.OMRSAuditCode;
import org.apache.atlas.omrs.auditlog.OMRSAuditLog;
import org.apache.atlas.omrs.auditlog.OMRSAuditingComponent;
import org.apache.atlas.omrs.eventmanagement.events.v1.OMRSEventV1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * OMRSTopicConnector provides the support for the registration of listeners and the distribution of
 * incoming events to the registered listeners.  An implementation of the OMRSTopicConnector needs to
 * extend this class to include the interaction with the eventing/messaging layer.
 * <ul>
 *     <li>
 *         For inbound events it should call the protected distributeEvents() method.
 *     </li>
 *     <li>
 *         For outbound events, callers will invoke the sendEvent() method.
 *     </li>
 *     <li>
 *         When the server no longer needs the topic, it will call close().
 *     </li>
 * </ul>
 */
public abstract class OMRSTopicConnector extends ConnectorBase implements OMRSTopic, Runnable
{
    ArrayList<OMRSTopicListener> topicListeners     = new  ArrayList<>();

    private static final Logger       log      = LoggerFactory.getLogger(OMRSTopicConnector.class);
    private static final OMRSAuditLog auditLog = new OMRSAuditLog(OMRSAuditingComponent.OMRS_TOPIC_CONNECTOR);

    private static final String       defaultThreadName = "OMRSTopicListener";
    private static final String       defaultTopicName  = "OMRSTopic";

    private volatile boolean keepRunning = false;

    private String   listenerThreadName = defaultThreadName;
    private String   topicName = defaultTopicName;
    private int      sleepTime = 100;

    /**
     * Simple constructor
     */
    public OMRSTopicConnector()
    {
        super();
    }


    /**
     * Return the name of the topic for this connector.
     *
     * @return String topic name.
     */
    public String getTopicName()
    {
        return topicName;
    }

    /**
     * This is the method called by the listener thread when it starts.
     */
    public void run()
    {
        OMRSAuditCode auditCode = OMRSAuditCode.OMRS_TOPIC_LISTENER_START;
        auditLog.logRecord(listenerThreadName,
                           auditCode.getLogMessageId(),
                           auditCode.getSeverity(),
                           auditCode.getFormattedLogMessage(topicName),
                           this.getConnection().toString(),
                           auditCode.getSystemAction(),
                           auditCode.getUserAction());

        while (keepRunning)
        {
            try
            {
                List<OMRSEventV1> receivedEvents = this.checkForEvents();

                if (receivedEvents != null)
                {
                    for (OMRSEventV1  event : receivedEvents)
                    {
                        if (event != null)
                        {
                            this.distributeEvent(event);
                        }
                    }
                }
                else
                {
                    Thread.sleep(sleepTime);
                }
            }
            catch (InterruptedException   wakeUp)
            {

            }
        }

        auditCode = OMRSAuditCode.OMRS_TOPIC_LISTENER_SHUTDOWN;
        auditLog.logRecord(listenerThreadName,
                           auditCode.getLogMessageId(),
                           auditCode.getSeverity(),
                           auditCode.getFormattedLogMessage(topicName),
                           this.getConnection().toString(),
                           auditCode.getSystemAction(),
                           auditCode.getUserAction());
    }


    /**
     * Pass an event that has been received on the topic to each of the registered listeners.
     *
     * @param event - OMRSEvent to distribute
     */
    protected void distributeEvent(OMRSEventV1 event)
    {
        for (OMRSTopicListener  topicListener : topicListeners)
        {
            try
            {
                topicListener.processEvent(event);
            }
            catch (Throwable  error)
            {
                final String   actionDescription = "distributeEvent";

                OMRSAuditCode auditCode = OMRSAuditCode.EVENT_PROCESSING_ERROR;
                auditLog.logRecord(actionDescription,
                                   auditCode.getLogMessageId(),
                                   auditCode.getSeverity(),
                                   auditCode.getFormattedLogMessage(event.toString(), error.toString()),
                                   null,
                                   auditCode.getSystemAction(),
                                   auditCode.getUserAction());
            }
        }
    }


    /**
     * Look to see if there is one of more new events to process.
     *
     * @return a list of received events or null
     */
    protected List<OMRSEventV1> checkForEvents()
    {
        return null;
    }


    /**
     * Register a listener object.  This object will be supplied with all of the events received on the topic.
     *
     * @param topicListener - object implementing the OMRSTopicListener interface
     */
    public void registerListener(OMRSTopicListener  topicListener)
    {
        if (topicListener != null)
        {
            topicListeners.add(topicListener);
        }
    }


    /**
     * Indicates that the connector is completely configured and can begin processing.
     *
     * @throws ConnectorCheckedException - there is a problem within the connector.
     */
    public void start() throws ConnectorCheckedException
    {
        super.start();

        keepRunning = true;

        if (super.connection != null)
        {
            Endpoint endpoint = super.connection.getEndpoint();

            if (endpoint != null)
            {
                topicName = endpoint.getAddress();
                listenerThreadName = defaultThreadName + ": " + topicName;
            }
        }

        Thread listenerThread = new Thread(this, listenerThreadName);
        listenerThread.start();
    }


    /**
     * Free up any resources held since the connector is no longer needed.
     *
     * @throws ConnectorCheckedException - there is a problem within the connector.
     */
    public  void disconnect() throws ConnectorCheckedException
    {
        super.disconnect();

        keepRunning = false;
    }
}
