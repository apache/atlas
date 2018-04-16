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
package org.apache.atlas.omas.connectedasset.properties;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * The RelatedMediaType defines the type of resource referenced in a related media reference.
 * <ul>
 *     <li>Image</li>
 *     <li>Audio</li>
 *     <li>Document</li>
 *     <li>Video</li>
 *     <li>Other</li>
 * </ul>
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public enum RelatedMediaType implements Serializable
{
    IMAGE(0, "Image"),
    AUDIO(1, "Audio"),
    DOCUMENT(2, "Document"),
    VIDEO(3, "Video"),
    OTHER(99, "Other");

    private static final long     serialVersionUID = 1L;

    private int            mediaTypeCode;
    private String         mediaTypeName;


    /**
     * Typical Constructor
     */
    RelatedMediaType(int     mediaTypeCode, String   mediaTypeName)
    {
        /*
         * Save the values supplied
         */
        this.mediaTypeCode = mediaTypeCode;
        this.mediaTypeName = mediaTypeName;

    }


    /**
     * Return the code for this enum instance
     *
     * @return int - media type code
     */
    public int getMediaUsageCode()
    {
        return mediaTypeCode;
    }


    /**
     * Return the default name for this enum instance.
     *
     * @return String - default name
     */
    public String getMediaUsageName()
    {
        return mediaTypeName;
    }
}