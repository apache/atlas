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
 * NoteLog manages a list of notes for an asset
 */
public class NoteLog extends Referenceable
{
    /*
     * Attributes of an note log
     */
    private String displayName = null;
    private String description = null;
    private Notes  notes = null;


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
     * @param displayName - consumable name
     * @param description - description property stored for the note log.
     * @param notes - list of notes for this note log.
     */
    public NoteLog(AssetDescriptor      parentAsset,
                   ElementType          type,
                   String               guid,
                   String               url,
                   Classifications      classifications,
                   String               qualifiedName,
                   AdditionalProperties additionalProperties,
                   Meanings             meanings,
                   String               displayName,
                   String               description,
                   Notes                notes)
    {
        super(parentAsset, type, guid, url, classifications, qualifiedName, additionalProperties, meanings);

        this.displayName = displayName;
        this.description = description;
        this.notes = notes;
    }


    /**
     * Copy/clone constructor.
     *
     * @param parentAsset - descriptor for parent asset
     * @param templateNotelog - note log to copy
     */
    public NoteLog(AssetDescriptor parentAsset, NoteLog templateNotelog)
    {
        /*
         * Initialize the super class.
         */
        super(parentAsset, templateNotelog);

        if (templateNotelog != null)
        {
            /*
             * Copy the values from the supplied template.
             */
            displayName = templateNotelog.getDisplayName();
            description = templateNotelog.getDescription();

            Notes     templateNotes = templateNotelog.getNotes();
            if (templateNotes != null)
            {
                notes = templateNotes.cloneIterator(parentAsset);
            }
        }
    }


    /**
     * Returns the stored display name property for the note log.
     * If no display name is available then null is returned.
     *
     * @return displayName
     */
    public String getDisplayName()
    {
        return displayName;
    }


    /**
     * Returns the stored description property for the note log.
     * If no description is provided then null is returned.
     *
     * @return description
     */
    public String getDescription()
    {
        return description;
    }


    /**
     * Return the list of notes defined for this note log.
     *
     * @return Notes - list of notes
     */
    public Notes getNotes()
    {
        if (notes == null)
        {
            return notes;
        }
        else
        {
            return notes.cloneIterator(super.getParentAsset());
        }
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "NoteLog{" +
                "displayName='" + displayName + '\'' +
                ", description='" + description + '\'' +
                ", notes=" + notes +
                ", qualifiedName='" + qualifiedName + '\'' +
                ", additionalProperties=" + additionalProperties +
                ", meanings=" + meanings +
                ", type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}