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

import java.util.Date;

/**
 * Note defines the properties of a single note in a note log.
 */
public class Note extends Referenceable
{
    /*
     * Attributes of a Note
     */
    private String text = null;
    private Date   lastUpdate = null;
    private String user = null;


    /**
     * Typical Constructor
     *
     * @param parentAsset - descriptor for parent asset
     * @param type - details of the metadata type for this properties object
     * @param guid - String - unique id
     * @param url - String - URL
     * @param classifications - enumeration of classifications
     * @param qualifiedName - unique name
     * @param additionalProperties - additional properties for the referenceable object.
     * @param meanings - list of glossary terms (summary)
     * @param text - the text of the note
     * @param lastUpdate - the last update date for the note.
     * @param user - the user id of the person who created the note
     */
    public Note(AssetDescriptor      parentAsset,
                ElementType          type,
                String               guid,
                String               url,
                Classifications      classifications,
                String               qualifiedName,
                AdditionalProperties additionalProperties,
                Meanings             meanings,
                String               text,
                Date                 lastUpdate,
                String               user)
    {
        super(parentAsset, type, guid, url, classifications, qualifiedName, additionalProperties, meanings);

        this.text = text;
        this.lastUpdate = lastUpdate;
        this.user = user;
    }

    /**
     * Copy/clone constructor.
     *
     * @param parentAsset - descriptor for parent asset
     * @param templateNote - note to copy
     */
    public Note(AssetDescriptor parentAsset, Note templateNote)
    {
        /*
         * Save the parent asset description.
         */
        super(parentAsset, templateNote);

        if (templateNote != null)
        {
            /*
             * Copy the properties from the supplied note.
             */
            text = templateNote.getText();
            user = templateNote.getUser();

            Date templateLastUpdate = templateNote.getLastUpdate();
            if (templateLastUpdate != null)
            {
                lastUpdate = new Date(templateLastUpdate.getTime());
            }
        }
    }


    /**
     * Return the text of the note.
     *
     * @return String text
     */
    public String getText() { return text; }


    /**
     * Return the last time a change was made to this note.
     *
     * @return Date last update
     */
    public Date getLastUpdate()
    {
        if (lastUpdate == null)
        {
            return lastUpdate;
        }
        else
        {
            return new Date(lastUpdate.getTime());
        }
    }


    /**
     * Return the user id of the person who created the note.  Null means the user id is not known.
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
        return "Note{" +
                "text='" + text + '\'' +
                ", lastUpdate=" + lastUpdate +
                ", user='" + user + '\'' +
                ", qualifiedName='" + qualifiedName + '\'' +
                ", additionalProperties=" + additionalProperties +
                ", meanings=" + meanings +
                ", type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}