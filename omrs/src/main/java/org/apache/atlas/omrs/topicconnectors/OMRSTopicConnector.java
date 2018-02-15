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
import org.apache.atlas.omrs.eventmanagement.events.v1.OMRSEventV1;

import java.util.ArrayList;


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
public abstract class OMRSTopicConnector extends ConnectorBase implements OMRSTopic
{
    ArrayList<OMRSTopicListener> topicListeners     = new  ArrayList<>();

    /**
     * Simple constructor
     */
    public OMRSTopicConnector()
    {
        /*
         * Nothing to do
         */
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
                // TODO Need to log error
            }
        }
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
     * Free up any resources held since the connector is no longer needed.
     *
     * @throws ConnectorCheckedException - there is a problem disconnecting the connector.
     */
    public void disconnect() throws ConnectorCheckedException
    {

    }
}
