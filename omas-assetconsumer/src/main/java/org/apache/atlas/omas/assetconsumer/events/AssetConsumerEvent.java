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
package org.apache.atlas.omas.assetconsumer.events;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.atlas.omas.assetconsumer.properties.Asset;

import java.io.Serializable;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * AssetConsumerEvent describes the structure of the events emitted by the Asset Consumer OMAS.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class AssetConsumerEvent implements Serializable
{
    AssetConsumerEventType   eventType = AssetConsumerEventType.UNKNOWN_ASSET_CONSUMER_EVENT;
    Asset                    asset = null;

    private static final long     serialVersionUID = 1L;

    /**
     * Default constructor
     */
    public AssetConsumerEvent()
    {
    }


    /**
     * Return the type of event.
     *
     * @return event type enum
     */
    public AssetConsumerEventType getEventType()
    {
        return eventType;
    }


    /**
     * Set up the type of event.
     *
     * @param eventType - event type enum
     */
    public void setEventType(AssetConsumerEventType eventType)
    {
        this.eventType = eventType;
    }


    /**
     * Return the asset description.
     *
     * @return properties about the asset
     */
    public Asset getAsset()
    {
        return asset;
    }


    /**
     * Set up the asset description.
     *
     * @param asset - properties about the asset.
     */
    public void setAsset(Asset asset)
    {
        this.asset = asset;
    }
}
