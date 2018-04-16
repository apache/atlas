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
import org.apache.atlas.omas.connectedasset.ffdc.ConnectedAssetErrorCode;
import org.apache.atlas.omas.connectedasset.ffdc.exceptions.ConnectedAssetRuntimeException;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * InformalTag stores information about a tag connected to an asset.
 * InformalTags provide informal classifications to assets
 * and can be added at any time.
 *
 * InformalTags have the userId of the person who added the tag, the name of the tag and its description.
 *
 * The content of the tag is a personal judgement (which is why the user's id is in the tag)
 * and there is no formal review of the tags.  However, they can be used as a basis for crowd-sourcing
 * Glossary terms.
 *
 * Private InformalTags are only returned to the user that created them.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class InformalTag extends ElementHeader
{
    /*
     * Attributes of a InformalTag
     */
    private boolean isPrivateTag = false;

    private String name = null;
    private String description = null;
    private String user = null;


    /**
     * Default Constructor
     */
    public InformalTag()
    {
        super();
    }


    /**
     * Copy/clone constructor.
     *
     * @param templateInformalTag - element to copy
     */
    public InformalTag(InformalTag templateInformalTag)
    {
        /*
         * Save the parent asset description.
         */
        super(templateInformalTag);

        if (templateInformalTag != null)
        {
            /*
             * Copy the values from the supplied tag.
             */
            isPrivateTag = templateInformalTag.isPrivateTag();
            user = templateInformalTag.getUser();
            name = templateInformalTag.getName();
            description = templateInformalTag.getDescription();
        }
    }


    /**
     * Return boolean flag to say whether the tag is private or not.  A private tag is only seen by the
     * person who set it up.  Public tags are visible to everyone how can sse the asset description.
     *
     * @return boolean - is private flag
     */
    public boolean isPrivateTag() {
        return isPrivateTag;
    }


    /**
     * Set up boolean flag to say whether the tag is private or not.  A private tag is only seen by the
     * person who set it up.  Public tags are visible to everyone how can sse the asset description.
     *
     * @param privateTag - boolean - is private flag
     */
    public void setPrivateTag(boolean privateTag) {
        isPrivateTag = privateTag;
    }


    /**
     * Return the user id of the person who created the tag.  Null means the user id is not known.
     *
     * @return String - tagging user
     */
    public String getUser() {
        return user;
    }


    /**
     * Set up the user id of the person who created the tag. Null means the user id is not known.
     *
     * @param taggingUser - String - tagging user
     */
    public void setUser(String taggingUser) {
        this.user = taggingUser;
    }


    /**
     * Return the name of the tag.  It is not valid to have a tag with no name.  However, there is a point where
     * the tag object is created and the tag name not set, so null is a possible response.
     *
     * @return String - tag name
     */
    public String getName() {
        return name;
    }


    /**
     * Set up the name of the tag.  It is not valid to have a tag with no name.
     *
     * @param tagName String - tag name
     */
    public void setName(String tagName)
    {
        final String  methodName = "setName";

        if (tagName == null || tagName.equals(""))
        {
            /*
             * Build and throw exception.  This should not happen - likely to be a problem in the
             * repository connector.
             */
            ConnectedAssetErrorCode errorCode = ConnectedAssetErrorCode.NULL_TAG_NAME;
            String       errorMessage = errorCode.getErrorMessageId()
                                      + errorCode.getFormattedErrorMessage(methodName,
                                                                           this.getClass().getName());

            throw new ConnectedAssetRuntimeException(errorCode.getHTTPErrorCode(),
                                                     this.getClass().getName(),
                                                     methodName,
                                                     errorMessage,
                                                     errorCode.getSystemAction(),
                                                     errorCode.getUserAction());
        }
        else
        {
            this.name = tagName;
        }
    }


    /**
     * Return the tag description - null means no description is available.
     *
     * @return String - tag description
     */
    public String getDescription()
    {
        return description;
    }


    /**
     * Set up the tag description - null means no description is available.
     *
     * @param tagDescription - String - tag description
     */
    public void setDescription(String tagDescription) {
        this.description = tagDescription;
    }
}