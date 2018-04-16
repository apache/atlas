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
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class AssetDetail extends AssetSummary
{
    private AdditionalProperties        assetProperties        = null;
    private List<ExternalIdentifier>    externalIdentifiers    = null;
    private List<RelatedMediaReference> relatedMediaReferences = null;
    private List<NoteLog>               noteLogs               = null;
    private List<ExternalReference>     externalReferences     = null;
    private List<Connection>            connections            = null;
    private List<License>               licenses               = null;
    private List<Certification>         certifications         = null;


    /**
     * Typical constructor - initialize superclasses
     */
    public AssetDetail()
    {
        super();
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
            AdditionalProperties        templateAssetProperties = templateAssetDetail.getAssetProperties();
            List<ExternalIdentifier>    templateExternalIdentifiers = templateAssetDetail.getExternalIdentifiers();
            List<RelatedMediaReference> templateRelatedMediaReferences = templateAssetDetail.getRelatedMediaReferences();
            List<NoteLog>               templateNoteLogs = templateAssetDetail.getNoteLogs();
            List<ExternalReference>     templateExternalReferences = templateAssetDetail.getExternalReferences();
            List<Connection>            templateConnections = templateAssetDetail.getConnections();
            List<License>               templateLicenses = templateAssetDetail.getLicenses();
            List<Certification>         templateCertifications = templateAssetDetail.getCertifications();

            if (templateAssetProperties != null)
            {
                assetProperties = new AdditionalProperties(templateAssetProperties);
            }
            if (templateExternalIdentifiers != null)
            {
                externalIdentifiers = new ArrayList<>(templateExternalIdentifiers);
            }
            if (templateRelatedMediaReferences != null)
            {
                relatedMediaReferences = new ArrayList<>(templateRelatedMediaReferences);
            }
            if (templateNoteLogs != null)
            {
                noteLogs = new ArrayList<>(templateNoteLogs);
            }
            if (templateExternalReferences != null)
            {
                externalReferences = new ArrayList<>(templateExternalReferences);
            }
            if (templateConnections != null)
            {
                connections = new ArrayList<>(templateConnections);
            }
            if (templateLicenses != null)
            {
                licenses = new ArrayList<>(templateLicenses);
            }
            if (templateCertifications != null)
            {
                certifications = new ArrayList<>(templateCertifications);
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
            return new AdditionalProperties(assetProperties);
        }
    }


    /**
     * Set up the asset properties for this asset.
     *
     * @param assetProperties - AdditionalProperties object
     */
    public void setAssetProperties(AdditionalProperties assetProperties)
    {
        this.assetProperties = assetProperties;
    }


    /**
     * Return an enumeration of the external identifiers for this asset (or null if none).
     *
     * @return ExternalIdentifiers list
     */
    public List<ExternalIdentifier> getExternalIdentifiers()
    {
        if (externalIdentifiers == null)
        {
            return externalIdentifiers;
        }
        else
        {
            return new ArrayList<>(externalIdentifiers);
        }
    }


    /**
     * Set up the external identifiers for this asset.
     *
     * @param externalIdentifiers - ExternalIdentifiers list
     */
    public void setExternalIdentifiers(List<ExternalIdentifier> externalIdentifiers)
    {
        this.externalIdentifiers = externalIdentifiers;
    }


    /**
     * Return an enumeration of references to the related media associated with this asset.
     *
     * @return RelatedMediaReferences List
     */
    public List<RelatedMediaReference> getRelatedMediaReferences()
    {
        if (relatedMediaReferences == null)
        {
            return relatedMediaReferences;
        }
        else
        {
            return new ArrayList<>(relatedMediaReferences);
        }
    }


    /**
     * Set up the enumeration of references to the related media associated with this asset.
     *
     * @param relatedMediaReferences - RelatedMediaReferences list
     */
    public void setRelatedMediaReferences(List<RelatedMediaReference> relatedMediaReferences)
    {
        this.relatedMediaReferences = relatedMediaReferences;
    }


    /**
     * Return an enumeration of NoteLogs linked to this asset.
     *
     * @return Notelogs list
     */
    public List<NoteLog> getNoteLogs()
    {
        if (noteLogs == null)
        {
            return noteLogs;
        }
        else
        {
            return new ArrayList<>(noteLogs);
        }
    }


    /**
     * Set up the list of note logs for this asset.
     *
     * @param noteLogs - NoteLogs list
     */
    public void setNoteLogs(List<NoteLog> noteLogs)
    {
        this.noteLogs = noteLogs;
    }


    /**
     * Return the list of external references associated with this asset.
     *
     * @return ExternalReferences list
     */
    public List<ExternalReference> getExternalReferences()
    {
        if (externalReferences == null)
        {
            return externalReferences;
        }
        else
        {
            return new ArrayList<>(externalReferences);
        }
    }


    /**
     * Set up the external references for this asset.
     *
     * @param externalReferences - ExternalReferences list
     */
    public void setExternalReferences(List<ExternalReference> externalReferences)
    {
        this.externalReferences = externalReferences;
    }


    /**
     * Return an enumeration of the connections defined for this asset.
     *
     * @return Connections enumeration
     */
    public List<Connection> getConnections()
    {
        if (connections == null)
        {
            return connections;
        }
        else
        {
            return new ArrayList<>(connections);
        }
    }


    /**
     * Set up the list of connections defined for accessing this asset.
     *
     * @param connections - Connections list
     */
    public void setConnections(ArrayList<Connection> connections)
    {
        this.connections = connections;
    }


    /**
     * Return the list of licenses associated with the asset.
     *
     * @return Licenses
     */
    public List<License> getLicenses()
    {
        if (licenses == null)
        {
            return licenses;
        }
        else
        {
            return new ArrayList<>(licenses);
        }
    }


    /**
     * Set up the licenses associated with this asset.
     *
     * @param licenses - List of licenses
     */
    public void setLicenses(List<License> licenses) { this.licenses = licenses; }


    /**
     * Return the list of certifications awarded to the asset.
     *
     * @return Certifications - list of certifications
     */
    public List<Certification> getCertifications()
    {
        if (certifications == null)
        {
            return certifications;
        }
        else
        {
            return new ArrayList<>(certifications);
        }
    }


    /**
     * Set up the list of certifications awarded to the asset.
     *
     * @param certifications - Certifications - list of certifications
     */
    public void setCertifications(List<Certification> certifications) { this.certifications = certifications; }
}
