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

package org.apache.atlas.omas.connectedasset.properties;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

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
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class AssetUniverse extends AssetDetail
{
    private List<Meaning>      meanings       = null;
    private SchemaElement      schema         = null;
    private Analysis           analysis       = null;
    private Feedback           feedback       = null;
    private List<Location>     knownLocations = null;
    private Lineage            lineage        = null;
    private List<RelatedAsset> relatedAssets  = null;


    /**
     * Typical Constructor
     */
    public AssetUniverse()
    {
        /*
         * Initialize the super classes
         */
        super();
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
            List<Meaning>      templateMeanings      = templateAssetUniverse.getMeanings();
            SchemaElement      templateSchema        = templateAssetUniverse.getSchema();
            Analysis           templateAnalysis      = templateAssetUniverse.getAnalysis();
            Feedback           templateFeedback      = templateAssetUniverse.getFeedback();
            List<Location>     templateLocations     = templateAssetUniverse.getKnownLocations();
            Lineage            templateLineage       = templateAssetUniverse.getLineage();
            List<RelatedAsset> templateRelatedAssets = templateAssetUniverse.getRelatedAssets();

            if (templateMeanings != null)
            {
                meanings = new ArrayList<>(templateMeanings);
            }
            if (templateSchema != null)
            {
                if (templateSchema.getType().equals("Schema"))
                {
                    schema = new Schema((Schema) templateSchema);
                }
                else
                {
                    schema = new PrimitiveSchemaElement((PrimitiveSchemaElement) templateSchema);
                }
            }
            if (templateAnalysis != null)
            {
                analysis = new Analysis(templateAnalysis);
            }
            if (templateFeedback != null)
            {
                feedback = new Feedback(templateFeedback);
            }
            if (templateLocations != null)
            {
                knownLocations = new ArrayList<>(templateLocations);
            }
            if (templateLineage != null)
            {
                lineage = new Lineage(templateLineage);
            }
            if (templateRelatedAssets != null)
            {
                relatedAssets = new ArrayList<>(templateRelatedAssets);
            }
        }
    }


    /**
     * Return the list of glossary definitions assigned directly to this asset.
     *
     * @return Meanings - list of glossary definitions.
     */
    public List<Meaning> getMeanings()
    {
        if (meanings == null)
        {
            return meanings;
        }
        else
        {
            return new ArrayList<>(meanings);
        }
    }


    /**
     * Set up definitions assigned to the asset.
     *
     * @param meanings - Meanings - list of glossary definitions.
     */
    public void setMeanings(List<Meaning> meanings) { this.meanings = meanings; }


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
            return schema.cloneSchemaElement();
        }
    }


    /**
     * Set up the schema object for this connected asset.  This should contain the schema itself and
     * the glossary definitions linked to the schema elements.
     *
     * @param schema - Schema object to query schema and related glossary definitions.
     */
    public void setSchema(SchemaElement schema) {
        this.schema = schema;
    }



    /**
     * Return details of the metadata discovery analysis for the asset.
     *
     * @return Analysis - analysis object to query the annotations
     */
    public Analysis getAnalysis()
    {
        if (analysis == null)
        {
            return analysis;
        }
        else
        {
            return new Analysis(analysis);
        }
    }


    /**
     * Set up the analysis object for the connected asset.  This should contain all of the annotations associated
     * with the asset.
     *
     * @param analysis - Analysis object to query the annotations
     */
    public void setAnalysis(Analysis analysis)
    {
        this.analysis = analysis;
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
            return new Feedback(feedback);
        }
    }


    /**
     * Set up the feedback object for the connected asset.  This should contain all of the feedback associated
     * with the asset.
     *
     * @param feedback - Feedback object to query the feedback.
     */
    public void setFeedback(Feedback feedback) {
        this.feedback = feedback;
    }


    /**
     * Set up the list of locations for the asset.
     *
     * @return Locations - list of locations.
     */
    public List<Location> getKnownLocations()
    {
        if (knownLocations == null)
        {
            return knownLocations;
        }
        else
        {
            return new ArrayList<>(knownLocations);
        }
    }


    /**
     * Set up the list of locations for the assets.
     *
     * @param knownLocations - Locations list
     */
    public void setKnownLocations(List<Location> knownLocations) { this.knownLocations = knownLocations; }


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
            return new Lineage(lineage);
        }
    }


    /**
     * Set up the lineage object for the asset.
     *
     * @param lineage - lineage object to query the origin of the asset.
     */
    public void setLineage(Lineage lineage) {
        this.lineage = lineage;
    }


    /**
     * Return the list of assets related to this asset.
     *
     * @return RelatedAssets list
     */
    public List<RelatedAsset> getRelatedAssets()
    {
        if (relatedAssets == null)
        {
            return relatedAssets;
        }
        else
        {
            return new ArrayList<>(relatedAssets);
        }
    }


    /**
     * Set up the related assets for this asset.
     *
     * @param relatedAssets - RelatedAssets list
     */
    public void setRelatedAssets(List<RelatedAsset> relatedAssets) { this.relatedAssets = relatedAssets; }
}