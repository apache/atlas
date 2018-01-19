/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.ocf.properties;

/**
 * AssetUniverse extends AssetDetail which extend AssetSummary.  AssetUniverse adds information about the
 * common open metadata entities related to this asset.
 * <ul>
 *     <li>Meanings - glossary term(s) assigned to this asset.</li>
 *     <li>Schema - details of the schema associated with the asset.</li>
 *     <li>Analysis - details of the annotations added by the discovery services.</li>
 *     <li>Feedback - details of the people, products and feedback that are connected to the asset.</li>
 *     <li>Locations - details of the known locations of the asset.</li>
 *     <li>Lineage - details of the lineage for the asset.</li>
 *     <li>Related Assets - details of the assets lined to this asset.</li>
 * </ul>
 *
 */
public class AssetUniverse extends AssetDetail
{
    private   Meanings         meanings = null;
    private   SchemaElement    schema = null;
    private   Annotations      analysis = null;
    private   Feedback         feedback = null;
    private   Locations        knownLocations = null;
    private   Lineage          lineage = null;
    private   RelatedAssets    relatedAssets = null;


    /**
     * Typical Constructor
     *
     * @param type - details of the metadata type for this asset
     * @param guid - guid property
     * @param url - element URL used to access its properties in the metadata repository.
     * @param qualifiedName - unique name
     * @param displayName - consumable name
     * @param description - description of the asset
     * @param shortDescription - short description from relationship with Connection
     * @param owner - owner name
     * @param classifications - enumeration of classifications
     * @param assetProperties - AdditionalProperties object
     * @param externalIdentifiers - ExternalIdentifiers enumeration
     * @param relatedMediaReferences - RelatedMediaReferences enumeration
     * @param noteLogs - NoteLogs iterator
     * @param externalReferences - ExternalReferences iterator
     * @param connections - List of connections attached to the asset
     * @param licenses - List of licenses
     * @param certifications - Certifications - list of certifications
     * @param meanings - Meanings - list of glossary definitions.
     * @param schema - Schema object to query schema and related glossary definitions.
     * @param analysis - Annotations from metadata discovery.
     * @param feedback - Feedback object to query the feedback.
     * @param knownLocations - Locations list
     * @param lineage - lineage object to query the origin of the asset.
     * @param relatedAssets - RelatedAssets list
     */
    public AssetUniverse(ElementType            type,
                         String                 guid,
                         String                 url,
                         String                 qualifiedName,
                         String                 displayName,
                         String                 shortDescription,
                         String                 description,
                         String                 owner,
                         Classifications        classifications,
                         AdditionalProperties   assetProperties,
                         ExternalIdentifiers    externalIdentifiers,
                         RelatedMediaReferences relatedMediaReferences,
                         NoteLogs               noteLogs,
                         ExternalReferences     externalReferences,
                         Connections            connections,
                         Licenses               licenses,
                         Certifications         certifications,
                         Meanings               meanings,
                         SchemaElement          schema,
                         Annotations            analysis,
                         Feedback               feedback,
                         Locations              knownLocations,
                         Lineage                lineage,
                         RelatedAssets          relatedAssets)
    {
        super(type,
              guid,
              url,
              qualifiedName,
              displayName,
              shortDescription,
              description,
              owner,
              classifications,
              assetProperties,
              externalIdentifiers,
              relatedMediaReferences,
              noteLogs,
              externalReferences,
              connections,
              licenses,
              certifications);

        this.meanings = meanings;
        this.schema = schema;
        this.analysis = analysis;
        this.feedback = feedback;
        this.knownLocations = knownLocations;
        this.lineage = lineage;
        this.relatedAssets = relatedAssets;
    }

    /**
     * Copy/clone Constructor - note this is a deep copy
     *
     * @param templateAssetUniverse - template to copy
     */
    public AssetUniverse(AssetUniverse   templateAssetUniverse)
    {
        /*
         * Initialize the super classes
         */
        super(templateAssetUniverse);

        /*
         * Set up the universe private variables.
         */
        if (templateAssetUniverse != null)
        {
            /*
             * Create the top-level property objects for this new asset using the values from the template.
             * The get methods create clones of the returned objects so no need to duplicate objects here.
             */
            Meanings      templateMeanings      = templateAssetUniverse.getMeanings();
            SchemaElement templateSchema        = templateAssetUniverse.getSchema();
            Annotations   templateAnalysis      = templateAssetUniverse.getAnalysis();
            Feedback      templateFeedback      = templateAssetUniverse.getFeedback();
            Locations     templateLocations     = templateAssetUniverse.getKnownLocations();
            Lineage       templateLineage       = templateAssetUniverse.getLineage();
            RelatedAssets templateRelatedAssets = templateAssetUniverse.getRelatedAssets();

            if (templateMeanings != null)
            {
                meanings = templateMeanings.cloneIterator(this);
            }
            if (templateSchema != null)
            {
                if (templateSchema.getType().equals("Schema"))
                {
                    schema = new Schema(this, (Schema) templateSchema);
                }
                else
                {
                    schema = new PrimitiveSchemaElement(this, (PrimitiveSchemaElement) templateSchema);
                }
            }
            if (templateAnalysis != null)
            {
                analysis = templateAnalysis.cloneIterator(this);
            }
            if (templateFeedback != null)
            {
                feedback = new Feedback(this, templateFeedback);
            }
            if (templateLocations != null)
            {
                knownLocations = templateLocations.cloneIterator(this);
            }
            if (templateLineage != null)
            {
                lineage = new Lineage(this, templateLineage);
            }
            if (templateRelatedAssets != null)
            {
                relatedAssets = templateRelatedAssets.cloneIterator(this);
            }
        }
    }


    /**
     * Return the list of glossary definitions assigned directly to this asset.
     *
     * @return Meanings - list of glossary definitions.
     */
    public Meanings getMeanings()
    {
        if (meanings == null)
        {
            return meanings;
        }
        else
        {
            return meanings.cloneIterator(this);
        }
    }


    /**
     * Return details of the schema associated with the asset.
     *
     * @return SchemaElement - schema object to query the schema associated with the connected asset.
     */
    public SchemaElement getSchema()
    {
        if (schema == null)
        {
            return schema;
        }
        else
        {
            return schema.cloneSchemaElement(this);
        }
    }


    /**
     * Return details of the metadata discovery analysis for the asset.
     *
     * @return Annotations - List of annotations from metadata discovery
     */
    public Annotations getAnalysis()
    {
        if (analysis == null)
        {
            return analysis;
        }
        else
        {
            return analysis.cloneIterator(this);
        }
    }


    /**
     * Return details of the people, products and feedback that are connected to the asset.
     *
     * @return Feedback - feedback object to query the feedback on the asset.
     */
    public Feedback getFeedback()
    {
        if (feedback == null)
        {
            return feedback;
        }
        else
        {
            return new Feedback(this, feedback);
        }
    }


    /**
     * Return the list of locations for the asset.
     *
     * @return Locations - list of locations.
     */
    public Locations getKnownLocations()
    {
        if (knownLocations == null)
        {
            return knownLocations;
        }
        else
        {
            return knownLocations.cloneIterator(this);
        }
    }


    /**
     * Return details of the lineage for the asset.
     *
     * @return Lineage  - lineage object that allows queries about the lineage of the asset.
     */
    public Lineage getLineage()
    {
        if (lineage == null)
        {
            return lineage;
        }
        else
        {
            return new Lineage(this, lineage);
        }
    }


    /**
     * Return the list of assets related to this asset.
     *
     * @return RelatedAssets list
     */
    public RelatedAssets getRelatedAssets()
    {
        if (relatedAssets == null)
        {
            return relatedAssets;
        }
        else
        {
            return relatedAssets.cloneIterator(this);
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
        return "AssetUniverse{" +
                "meanings=" + meanings +
                ", schema=" + schema +
                ", analysis=" + analysis +
                ", feedback=" + feedback +
                ", knownLocations=" + knownLocations +
                ", lineage=" + lineage +
                ", relatedAssets=" + relatedAssets +
                '}';
    }
}