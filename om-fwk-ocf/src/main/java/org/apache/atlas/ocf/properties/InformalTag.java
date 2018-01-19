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

import org.apache.atlas.ocf.ffdc.OCFErrorCode;
import org.apache.atlas.ocf.ffdc.OCFRuntimeException;

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
public class InformalTag extends ElementHeader
{
    /*
     * Attributes of a InformalTag
     */
    private boolean isPrivateTag = false;
    private String  name         = null;
    private String  description  = null;
    private String  user         = null;


    /**
     * Typical Constructor
     *
     * @param parentAsset - descriptor for parent asset
     * @param type - details of the metadata type for this properties object
     * @param guid - String - unique id
     * @param url - String - URL
     * @param classifications - enumeration of classifications
     * @param isPrivateTag - boolean flag to say whether the tag is private or not.
     *                     A private tag is only seen by the person who set it up.
     *                     Public tags are visible to everyone how can sse the asset description.
     * @param name - name of the tag.  It is not valid to have a tag with no name.
     * @param description - tag description
     * @param user - the user id of the person who created the tag. Null means the user id is not known.
     */
    public InformalTag(AssetDescriptor parentAsset,
                       ElementType     type,
                       String          guid,
                       String          url,
                       Classifications classifications,
                       boolean         isPrivateTag,
                       String          name,
                       String          description,
                       String          user)
    {
        super(parentAsset, type, guid, url, classifications);

        if (name == null || name.equals(""))
        {
            /*
             * Build and throw exception.  This should not happen - likely to be a problem in the
             * repository connector.
             */
            OCFErrorCode errorCode = OCFErrorCode.NULL_TAG_NAME;
            String       errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(super.getParentAssetName(),
                                                         super.getParentAssetTypeName());

            throw new OCFRuntimeException(errorCode.getHTTPErrorCode(),
                                          this.getClass().getName(),
                                          "setName",
                                          errorMessage,
                                          errorCode.getSystemAction(),
                                          errorCode.getUserAction());
        }

        this.isPrivateTag = isPrivateTag;
        this.name = name;
        this.description = description;
        this.user = user;
    }

    /**
     * Copy/clone constructor.
     *
     * @param parentAsset - descriptor for parent asset
     * @param templateInformalTag - element to copy
     */
    public InformalTag(AssetDescriptor parentAsset, InformalTag templateInformalTag)
    {
        /*
         * Save the parent asset description.
         */
        super(parentAsset, templateInformalTag);

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
     * person who set it up.  Public tags are visible to everyone who can see the asset description.
     *
     * @return boolean - is private flag
     */
    public boolean isPrivateTag() {
        return isPrivateTag;
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
     * Return the name of the tag.  It is not valid to have a tag with no name.  However, there is a point where
     * the tag object is created and the tag name not set, so null is a possible response.
     *
     * @return String - tag name
     */
    public String getName() {
        return name;
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
     * @param tagDescription  - tag description
     */
    public void setDescription(String tagDescription) {
        this.description = tagDescription;
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "InformalTag{" +
                "isPrivateTag=" + isPrivateTag +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", user='" + user + '\'' +
                ", type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}