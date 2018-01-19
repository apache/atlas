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
 * ExternalIdentifier stores information about an identifier for the asset that is used in an external system.
 * This is used for correlating information about the asset across different systems.
 */
public class ExternalIdentifier extends Referenceable
{
    /*
     * Attributes of an external identifier
     */
    private String            identifier = null;
    private String            description = null;
    private String            usage = null;
    private String            source = null;
    private KeyPattern        keyPattern = null;
    private Referenceable     scope = null;
    private String            scopeDescription = null;


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
     * @param identifier - the external identifier for the asset.
     * @param description - the description of the external identifier.
     * @param usage - usage guidance for this external identifier.
     * @param source - source description for this external identifier.
     * @param keyPattern enum - name of the key pattern used for this external identifier.
     * @param scope - Referenceable scope of this external identifier.  This depends on the key pattern.
     *              It may be a server definition, a reference data set or glossary term.
     * @param scopeDescription - description of the scope for this external identifier.
     */
    public ExternalIdentifier(AssetDescriptor      parentAsset,
                              ElementType          type,
                              String               guid,
                              String               url,
                              Classifications      classifications,
                              String               qualifiedName,
                              AdditionalProperties additionalProperties,
                              Meanings             meanings,
                              String               identifier,
                              String               description,
                              String               usage,
                              String               source,
                              KeyPattern           keyPattern,
                              Referenceable        scope,
                              String               scopeDescription)
    {
        super(parentAsset, type, guid, url, classifications, qualifiedName, additionalProperties, meanings);

        this.identifier = identifier;
        this.description = description;
        this.usage = usage;
        this.source = source;
        this.keyPattern = keyPattern;
        this.scope = scope;
        this.scopeDescription = scopeDescription;
    }

    /**
     * Copy/clone constructor.
     *
     * @param parentAsset - descriptor for parent asset
     * @param templateExternalIdentifier - element to copy
     */
    public ExternalIdentifier(AssetDescriptor parentAsset, ExternalIdentifier templateExternalIdentifier)
    {
        /*
         * Initialize the super class.
         */
        super(parentAsset, templateExternalIdentifier);

        if (templateExternalIdentifier != null)
        {
            /*
             * Copy the values from the supplied template.
             */
            identifier = templateExternalIdentifier.getIdentifier();
            description = templateExternalIdentifier.getDescription();
            usage = templateExternalIdentifier.getUsage();
            source = templateExternalIdentifier.getSource();
            keyPattern = templateExternalIdentifier.getKeyPattern();

            Referenceable  templateScope = templateExternalIdentifier.getScope();
            if (templateScope != null)
            {
                /*
                 * Ensure comment replies has this object's parent asset, not the template's.
                 */
                scope = new Referenceable(parentAsset, templateScope);
            }

            scopeDescription = templateExternalIdentifier.getScopeDescription();
        }
    }


    /**
     * Return the external identifier for this asset.
     *
     * @return String identifier
     */
    public String getIdentifier() { return identifier; }


    /**
     * Return the description of the external identifier.
     *
     * @return String description
     */
    public String getDescription() { return description; }


    /**
     * Return details of how, where and when this external identifier is used.
     *
     * @return String usage
     */
    public String getUsage() { return usage; }


    /**
     * Return details of the source system where this external identifier comes from.
     *
     * @return String server
     */
    public String getSource() { return source; }


    /**
     * Return the key pattern that is used with this external identifier.
     *
     * @return KeyPattern enum
     */
    public KeyPattern getKeyPattern() { return keyPattern; }


    /**
     * Return the scope of this external identifier.  This depends on the key pattern.  It may be a server definition,
     * a reference data set or glossary term.
     *
     * @return Referenceable scope
     */
    public Referenceable getScope()
    {
        if (scope == null)
        {
            return scope;
        }
        else
        {
            return new Referenceable(super.getParentAsset(), scope);
        }
    }


    /**
     * Return the text description of the scope for this external identifier.
     *
     * @return String scope description
     */
    public String getScopeDescription() { return scopeDescription; }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "ExternalIdentifier{" +
                "identifier='" + identifier + '\'' +
                ", description='" + description + '\'' +
                ", usage='" + usage + '\'' +
                ", source='" + source + '\'' +
                ", keyPattern=" + keyPattern +
                ", scope=" + scope +
                ", scopeDescription='" + scopeDescription + '\'' +
                ", qualifiedName='" + qualifiedName + '\'' +
                ", additionalProperties=" + additionalProperties +
                ", meanings=" + meanings +
                ", type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}