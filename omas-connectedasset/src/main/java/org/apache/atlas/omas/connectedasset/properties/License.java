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

import java.util.Date;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

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
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
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
     * Default constructor.
     */
    public License()
    {
        super();
    }


    /**
     * Copy/clone constructor.
     *
     * @param templateLicense - element to copy
     */
    public License(License    templateLicense)
    {
        super(templateLicense);

        if (templateLicense != null)
        {
            licenseTypeGUID = templateLicense.getLicenseTypeGUID();
            licenseTypeName = templateLicense.getLicenseName();
            licensee = templateLicense.getLicensee();
            summary = templateLicense.getSummary();

            ExternalReference  templateLink = templateLicense.getLink();
            if (templateLink != null)
            {
                link = new ExternalReference(templateLink);
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
     * Set up the unique id of the license type.
     *
     * @param licenseGUID - String license type GUID
     */
    public void setLicenseTypeGUID(String licenseGUID) { this.licenseTypeGUID = licenseGUID; }


    /**
     * Return the type of the license.
     *
     * @return String license type
     */
    public String getLicenseName() { return licenseTypeName; }


    /**
     * Set up the type of the license.
     *
     * @param licenseName - String license type
     */
    public void setLicenseName(String licenseName) { this.licenseTypeName = licenseName; }


    /**
     * Get the name of the organization or person that issued the license.
     *
     * @return String name
     */
    public String getLicensee() { return licensee; }


    /**
     * Set up the name of the organization or person that issued the license.
     *
     * @param licensee - String name
     */
    public void setLicensee(String licensee) { this.licensee = licensee; }


    /**
     * Return a brief summary of the license.
     *
     * @return String summary
     */
    public String getSummary() { return summary; }


    /**
     * Set up a brief summary of the license.
     *
     * @param summary - String
     */
    public void setSummary(String summary) { this.summary = summary; }


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
            return new ExternalReference(link);
        }
    }


    /**
     * Set up the link to the full text of the license.
     *
     * @param link - ExternalReference for full text
     */
    public void setLink(ExternalReference link) { this.link = link; }


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
     * Set up start date for the license.
     *
     * @param startDate - Date
     */
    public void setStartDate(Date startDate) { this.startDate = startDate; }


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
     * Set up the end date for the license.
     *
     * @param endDate - Date
     */
    public void setEndDate(Date endDate) { this.endDate = endDate; }


    /**
     * Return any special conditions that apply to the license - such as endorsements.
     *
     * @return String license conditions
     */
    public String getLicenseConditions() { return licenseConditions; }


    /**
     * Set up any special conditions that apply to the license - such as endorsements.
     *
     * @param conditions - String license conditions
     */
    public void setLicenseConditions(String conditions) { this.licenseConditions = conditions; }


    /**
     * Return the name of the person or organization that set up the license agreement for this asset.
     *
     * @return String name
     */
    public String getCreatedBy() { return createdBy; }


    /**
     * Set up the name of the person or organization that set up the license agreement for this asset.
     *
     * @param createdBy - String name
     */
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }


    /**
     * Return the name of the person or organization that is responsible for the correct management of the asset
     * according to the license.
     *
     * @return String name
     */
    public String getCustodian() { return custodian; }


    /**
     * Set up the name of the person or organization that is responsible for the correct management of the asset
     * according to the license.
     *
     * @param custodian - String name
     */
    public void setCustodian(String custodian) { this.custodian = custodian; }


    /**
     * Return the notes for the custodian.
     *
     * @return String notes
     */
    public String getNotes() { return notes; }


    /**
     * Set up the notes from the custodian.
     *
     * @param notes - String
     */
    public void setNotes(String notes) { this.notes = notes; }
}