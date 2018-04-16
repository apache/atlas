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

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * ExternalIdentifier stores information about an identifier for the asset that is used in an external system.
 * This is used for correlating information about the asset across different systems.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
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
     * Default Constructor
     */
    public ExternalIdentifier()
    {
        super();
    }


    /**
     * Copy/clone constructor.
     *
     * @param templateExternalIdentifier - element to copy
     */
    public ExternalIdentifier(ExternalIdentifier templateExternalIdentifier)
    {
        /*
         * Initialize the super class.
         */
        super(templateExternalIdentifier);

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
                scope = new Referenceable(templateScope);
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
     * Set up the external identifier for the asset.
     * @param identifier - String
     */
    public void setIdentifier(String identifier) { this.identifier = identifier; }


    /**
     * Return the description of the external identifier.
     *
     * @return String description
     */
    public String getDescription() { return description; }


    /**
     * Set up the description of the external identifier.
     *
     * @param description - String
     */
    public void setDescription(String description) { this.description = description; }


    /**
     * Return details of how, where and when this external identifier is used.
     *
     * @return String usage
     */
    public String getUsage() { return usage; }


    /**
     * Setup the usage guidance for this external identifier.
     *
     * @param usage - String
     */
    public void setUsage(String usage) { this.usage = usage; }


    /**
     * Return details of the source system where this external identifier comes from.
     *
     * @return String server
     */
    public String getSource() { return source; }


    /**
     * Set up the source description for this external identifier.
     *
     * @param source - String
     */
    public void setSource(String source) { this.source = source; }


    /**
     * Return the key pattern that is used with this external identifier.
     *
     * @return KeyPattern enum
     */
    public KeyPattern getKeyPattern() { return keyPattern; }


    /**
     * Set up the name of the key pattern used for this external identifier.
     *
     * @param keyPattern enum
     */
    public void setKeyPattern(KeyPattern keyPattern) { this.keyPattern = keyPattern; }


    /**
     * Return the scope of this external identifier.  This depends on the key pattern.  It may be a server definition,
     * a reference data set or glossary term.
     *
     * @return Referencable scope
     */
    public Referenceable getScope()
    {
        if (scope == null)
        {
            return scope;
        }
        else
        {
            return new Referenceable(scope);
        }
    }


    /**
     * Set up the scope description for this external identifier.
     *
     * @param scope - Referenceable
     */
    public void setScope(Referenceable scope) { this.scope = scope; }


    /**
     * Return the text description of the scope for this external identifier.
     *
     * @return String scope description
     */
    public String getScopeDescription() { return scopeDescription; }


    /**
     * Set up the description of the scope for this external identifier.
     *
     * @param scopeDescription - String
     */
    public void setScopeDescription(String scopeDescription) { this.scopeDescription = scopeDescription; }
}