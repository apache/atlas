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
package org.apache.atlas.ocf.properties;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * The RelatedMediaType defines the type of resource referenced in a related media reference.
 * <ul>
 *     <li>Image - The media is an image.</li>
 *     <li>Audio - The media is an audio recording.</li>
 *     <li>Document - The media is a text document - probably rich text.</li>
 *     <li>Video - The media is a video recording.</li>
 *     <li>Other - The media type is not supported.</li>
 * </ul>
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public enum RelatedMediaType implements Serializable
{
    IMAGE(0,    "Image",    "The media is an image."),
    AUDIO(1,    "Audio",    "The media is an audio recording."),
    DOCUMENT(2, "Document", "The media is a text document - probably rich text."),
    VIDEO(3,    "Video",    "The media is a video recording."),
    OTHER(99,   "Other",    "The media type is not supported.");

    private static final long     serialVersionUID = 1L;

    private int            mediaTypeCode;
    private String         mediaTypeName;
    private String         mediaTypeDescription;


    /**
     * Typical Constructor
     */
    RelatedMediaType(int     mediaTypeCode, String   mediaTypeName, String    mediaTypeDescription)
    {
        /*
         * Save the values supplied
         */
        this.mediaTypeCode = mediaTypeCode;
        this.mediaTypeName = mediaTypeName;
        this.mediaTypeDescription = mediaTypeDescription;
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


    /**
     * Return the default description for this enum instance.
     *
     * @return String - default description
     */
    public String getMediaTypeDescription()
    {
        return mediaTypeDescription;
    }
}