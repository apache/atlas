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

/**
 * The Like properties object records a single user's "like" of an asset.
 */
public class Like extends ElementHeader
{
    /*
     * Attributes of a Like
     */
    private String user = null;


    /**
     * Typical Constructor
     *
     * @param parentAsset - descriptor for parent asset
     * @param type - details of the metadata type for this properties object
     * @param guid - String - unique id
     * @param url - String - URL
     * @param classifications - enumeration of classifications
     * @param user - the user id of the person who created the like.
     */
    public Like(AssetDescriptor parentAsset,
                ElementType     type,
                String          guid,
                String          url,
                Classifications classifications,
                String          user)
    {
        super(parentAsset, type, guid, url, classifications);

        this.user = user;
    }


    /**
     * Copy/clone constructor.
     *
     * @param parentAsset - descriptor for parent asset
     * @param templateLike - element to copy
     */
    public Like(AssetDescriptor parentAsset, Like templateLike)
    {
        /*
         * Save the parent asset description.
         */
        super(parentAsset, templateLike);

        if (templateLike != null)
        {
            /*
             * Copy the user name from the supplied like.
             */
            user = templateLike.getUser();
        }
    }


    /**
     * Return the user id of the person who created the like.  Null means the user id is not known.
     *
     * @return String - liking user
     */
    public String getUser() {
        return user;
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "Like{" +
                "user='" + user + '\'' +
                ", type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}