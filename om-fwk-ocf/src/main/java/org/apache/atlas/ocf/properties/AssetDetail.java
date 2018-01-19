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
 * AssetDetail extends AssetSummary to provide all of the properties related to this asset.  It includes:
 * <ul>
 *     <li>AssetProperties - properties unique to the particular type of asset including any vendor-specific facets.</li>
 *     <li>ExternalIdentifiers - list of identifiers for this asset that are used in other systems.</li>
 *     <li>RelatedMediaReferences - list of links to external media (images, audio, video) about this asset.</li>
 *     <li>NoteLogs - list of NoteLogs for this asset, often providing more detail on how to use the asset and
 *                   its current status.</li>
 *     <li>ExternalReferences - list of links to additional information about this asset.</li>
 *     <li>Connections - list of connections defined to access this asset.</li>
 *     <li>Licenses - list of licenses associated with the asset.</li>
 *     <li>Certifications - list of certifications that have been awarded to this asset.</li>
 * </ul>
 */
public class AssetDetail extends AssetSummary
{
    private AdditionalProperties   assetProperties = null;
    private ExternalIdentifiers    externalIdentifiers = null;
    private RelatedMediaReferences relatedMediaReferences = null;
    private NoteLogs               noteLogs = null;
    private ExternalReferences     externalReferences = null;
    private Connections            connections = null;
    private Licenses               licenses = null;
    private Certifications         certifications = null;


    /**
     * Typical constructor - initialize superclasses
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
     */
    public AssetDetail(ElementType            type,
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
                       Certifications         certifications)
    {
        super(type,
              guid,
              url,
              qualifiedName,
              displayName,
              shortDescription,
              description,
              owner,
              classifications);

        this.assetProperties = assetProperties;
        this.externalIdentifiers = externalIdentifiers;
        this.relatedMediaReferences = relatedMediaReferences;
        this.noteLogs = noteLogs;
        this.externalReferences = externalReferences;
        this.connections = connections;
        this.licenses = licenses;
        this.certifications = certifications;
    }

    /**
     * Copy/clone constructor.  Note, this is a deep copy
     *
     * @param templateAssetDetail - template to copy
     */
    public AssetDetail(AssetDetail  templateAssetDetail)
    {
        /*
         * Initialize superclass
         */
        super(templateAssetDetail);

        /*
         * Copy details from template, ensuring the parentAsset is this object, not the template's.
         */
        if (templateAssetDetail != null)
        {
            AdditionalProperties   templateAssetProperties = templateAssetDetail.getAssetProperties();
            ExternalIdentifiers    templateExternalIdentifiers = templateAssetDetail.getExternalIdentifiers();
            RelatedMediaReferences templateRelatedMediaReferences = templateAssetDetail.getRelatedMediaReferences();
            NoteLogs               templateNoteLogs = templateAssetDetail.getNoteLogs();
            ExternalReferences     templateExternalReferences = templateAssetDetail.getExternalReferences();
            Connections            templateConnections = templateAssetDetail.getConnections();
            Licenses               templateLicenses = templateAssetDetail.getLicenses();
            Certifications         templateCertifications = templateAssetDetail.getCertifications();

            if (templateAssetProperties != null)
            {
                assetProperties = new AdditionalProperties(this, templateAssetProperties);
            }
            if (templateExternalIdentifiers != null)
            {
                externalIdentifiers = templateExternalIdentifiers.cloneIterator(this);
            }
            if (templateRelatedMediaReferences != null)
            {
                relatedMediaReferences = templateRelatedMediaReferences.cloneIterator(this);
            }
            if (templateNoteLogs != null)
            {
                noteLogs = templateNoteLogs.cloneIterator(this);
            }
            if (templateExternalReferences != null)
            {
                externalReferences = templateExternalReferences.cloneIterator(this);
            }
            if (templateConnections != null)
            {
                connections = templateConnections.cloneIterator(this);
            }
            if (templateLicenses != null)
            {
                licenses = templateLicenses.cloneIterator(this);
            }
            if (templateCertifications != null)
            {
                certifications = templateCertifications.cloneIterator(this);
            }
        }
    }


    /**
     * Return the set of properties that are specific to the particular type of asset.  The caller is given their
     * own copy of the property object.  The properties are named entityName.attributeName. The values are all strings.
     *
     * @return AdditionalProperties - asset properties using the name of attributes from the model.
     */
    public AdditionalProperties getAssetProperties()
    {
        if (assetProperties == null)
        {
            return assetProperties;
        }
        else
        {
            return new AdditionalProperties(this, assetProperties);
        }
    }


    /**
     * Return an enumeration of the external identifiers for this asset (or null if none).
     *
     * @return ExternalIdentifiers enumeration
     */
    public ExternalIdentifiers getExternalIdentifiers()
    {
        if (externalIdentifiers == null)
        {
            return externalIdentifiers;
        }
        else
        {
            return externalIdentifiers.cloneIterator(this);
        }
    }


    /**
     * Return an enumeration of references to the related media associated with this asset.
     *
     * @return RelatedMediaReferences enumeration
     */
    public RelatedMediaReferences getRelatedMediaReferences()
    {
        if (relatedMediaReferences == null)
        {
            return relatedMediaReferences;
        }
        else
        {
            return relatedMediaReferences.cloneIterator(this);
        }
    }


    /**
     * Return an enumeration of NoteLogs linked to this asset.
     *
     * @return Notelogs iterator
     */
    public NoteLogs getNoteLogs()
    {
        if (noteLogs == null)
        {
            return noteLogs;
        }
        else
        {
            return noteLogs.cloneIterator(this);
        }
    }


    /**
     * Return the enumeration of external references associated with this asset.
     *
     * @return ExternalReferences iterator
     */
    public ExternalReferences getExternalReferences()
    {
        if (externalReferences == null)
        {
            return externalReferences;
        }
        else
        {
            return externalReferences.cloneIterator(this);
        }
    }


    /**
     * Return an enumeration of the connections defined for this asset.
     *
     * @return Connections enumeration
     */
    public Connections getConnections()
    {
        if (connections == null)
        {
            return connections;
        }
        else
        {
            return connections.cloneIterator(this);
        }
    }


    /**
     * Return the list of licenses associated with the asset.
     *
     * @return Licenses
     */
    public Licenses getLicenses()
    {
        if (licenses == null)
        {
            return licenses;
        }
        else
        {
            return licenses.cloneIterator(this);
        }
    }


    /**
     * Return the list of certifications awarded to the asset.
     *
     * @return Certifications - list of certifications
     */
    public Certifications getCertifications()
    {
        if (certifications == null)
        {
            return certifications;
        }
        else
        {
            return certifications.cloneIterator(this);
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
        return "AssetDetail{" +
                "assetProperties=" + assetProperties +
                ", externalIdentifiers=" + externalIdentifiers +
                ", relatedMediaReferences=" + relatedMediaReferences +
                ", noteLogs=" + noteLogs +
                ", externalReferences=" + externalReferences +
                ", connections=" + connections +
                ", licenses=" + licenses +
                ", certifications=" + certifications +
                '}';
    }
}
