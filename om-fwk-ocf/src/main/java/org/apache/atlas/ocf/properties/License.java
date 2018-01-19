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
 * <p>
 *     The data economy brings licensing to data and metadata.  Even open data typically has a license.
 *     License stores the license permission associated with the asset.
 * </p>
 * <p>
 *     The license will define the permitted uses and other requirements for using the asset.
 * </p>
 *
 */
public class License extends Referenceable
{
    /*
     * properties of a license
     */
    private  String                 licenseTypeGUID = null;
    private  String                 licenseTypeName = null;
    private  String                 licensee = null;
    private  String                 summary = null;
    private  ExternalReference      link = null;
    private  Date                   startDate = null;
    private  Date                   endDate = null;
    private  String                 licenseConditions = null;
    private  String                 createdBy = null;
    private  String                 custodian = null;
    private  String                 notes = null;


    /**
     * Typical constructor.
     *
     * @param parentAsset - descriptor for parent asset
     * @param type - details of the metadata type for this properties object
     * @param guid - String - unique id
     * @param url - String - URL
     * @param classifications - enumeration of classifications
     * @param qualifiedName - unique name
     * @param additionalProperties - additional properties for the referenceable object.
     * @param meanings - list of glossary terms (summary)
     * @param licenseTypeGUID - license type GUID
     * @param licenseTypeName - the name of the type of the license.
     * @param licensee - name of the organization or person that issued the license
     * @param summary - a brief summary of the license.
     * @param link - ExternalReference for full text of the license
     * @param startDate - start date for the license.
     * @param endDate - the end date for the license - null if license does not expire.
     * @param licenseConditions  - any special conditions that apply to the license - such as endorsements.
     * @param createdBy - String name of the person or organization that set up the license agreement for this asset.
     * @param custodian - the name of the person or organization that is responsible for the correct management
     *                  of the asset according to the license.
     * @param notes - the notes from the custodian.
     */
    public License(AssetDescriptor      parentAsset,
                   ElementType          type,
                   String               guid,
                   String               url,
                   Classifications      classifications,
                   String               qualifiedName,
                   AdditionalProperties additionalProperties,
                   Meanings             meanings,
                   String               licenseTypeGUID,
                   String               licenseTypeName,
                   String               licensee,
                   String               summary,
                   ExternalReference    link,
                   Date                 startDate,
                   Date                 endDate,
                   String               licenseConditions,
                   String               createdBy,
                   String               custodian,
                   String               notes)
    {
        super(parentAsset, type, guid, url, classifications, qualifiedName, additionalProperties, meanings);

        this.licenseTypeGUID = licenseTypeGUID;
        this.licenseTypeName = licenseTypeName;
        this.licensee = licensee;
        this.summary = summary;
        this.link = link;
        this.startDate = startDate;
        this.endDate = endDate;
        this.licenseConditions = licenseConditions;
        this.createdBy = createdBy;
        this.custodian = custodian;
        this.notes = notes;
    }

    /**
     * Copy/clone constructor.
     *
     * @param parentAsset - descriptor for parent asset
     * @param templateLicense - element to copy
     */
    public License(AssetDescriptor   parentAsset, License    templateLicense)
    {
        super(parentAsset, templateLicense);

        if (templateLicense != null)
        {
            licenseTypeGUID = templateLicense.getLicenseTypeGUID();
            licenseTypeName = templateLicense.getLicenseName();
            licensee = templateLicense.getLicensee();
            summary = templateLicense.getSummary();

            ExternalReference  templateLink = templateLicense.getLink();
            if (templateLink != null)
            {
                link = new ExternalReference(parentAsset, templateLink);
            }

            Date               templateStartDate = templateLicense.getStartDate();
            if (templateStartDate != null)
            {
                startDate = new Date(templateStartDate.getTime());
            }

            Date               templateEndDate = templateLicense.getEndDate();
            if (templateEndDate != null)
            {
                endDate = new Date(templateStartDate.getTime());
            }

            licenseConditions = templateLicense.getLicenseConditions();
            createdBy = templateLicense.getCreatedBy();
            custodian = templateLicense.getCustodian();
            notes = templateLicense.getNotes();
        }
    }


    /**
     * Return the unique id for the type of license.
     *
     * @return String license type GUID
     */
    public String getLicenseTypeGUID() { return licenseTypeGUID; }


    /**
     * Return the type of the license.
     *
     * @return String license type
     */
    public String getLicenseName() { return licenseTypeName; }


    /**
     * Get the name of the organization or person that issued the license.
     *
     * @return String name
     */
    public String getLicensee() { return licensee; }


    /**
     * Return a brief summary of the license.
     *
     * @return String summary
     */
    public String getSummary() { return summary; }


    /**
     * Return the link to the full text of the license.
     *
     * @return ExternalReference for full text
     */
    public ExternalReference getLink()
    {
        if (link == null)
        {
            return link;
        }
        else
        {
            return new ExternalReference(super.getParentAsset(), link);
        }
    }


    /**
     * Return the start date for the license.
     *
     * @return Date
     */
    public Date getStartDate()
    {
        if (startDate == null)
        {
            return startDate;
        }
        else
        {
            return new Date(startDate.getTime());
        }
    }


    /**
     * Return the end date for the license.
     *
     * @return Date
     */
    public Date getEndDate()
    {
        if (endDate == null)
        {
            return endDate;
        }
        else
        {
            return new Date(endDate.getTime());
        }
    }


    /**
     * Return any special conditions that apply to the license - such as endorsements.
     *
     * @return String license conditions
     */
    public String getLicenseConditions() { return licenseConditions; }


    /**
     * Return the name of the person or organization that set up the license agreement for this asset.
     *
     * @return String name
     */
    public String getCreatedBy() { return createdBy; }


    /**
     * Return the name of the person or organization that is responsible for the correct management of the asset
     * according to the license.
     *
     * @return String name
     */
    public String getCustodian() { return custodian; }


    /**
     * Return the notes for the custodian.
     *
     * @return String notes
     */
    public String getNotes() { return notes; }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "License{" +
                "licenseTypeGUID='" + licenseTypeGUID + '\'' +
                ", licenseTypeName='" + licenseTypeName + '\'' +
                ", licensee='" + licensee + '\'' +
                ", summary='" + summary + '\'' +
                ", link=" + link +
                ", startDate=" + startDate +
                ", endDate=" + endDate +
                ", licenseConditions='" + licenseConditions + '\'' +
                ", createdBy='" + createdBy + '\'' +
                ", custodian='" + custodian + '\'' +
                ", notes='" + notes + '\'' +
                ", qualifiedName='" + qualifiedName + '\'' +
                ", additionalProperties=" + additionalProperties +
                ", meanings=" + meanings +
                ", type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}