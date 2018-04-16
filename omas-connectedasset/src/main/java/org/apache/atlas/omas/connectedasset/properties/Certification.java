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
 *     Certification stores the certifications awarded to the asset.
 * </p>
 * <p>
 *     Many regulations and industry bodies define certifications that can confirm a level of support,
 *     capability or competence in an aspect of a digital organization’s operation.
 *     Having certifications may be necessary to operating legally or may be a business advantage.
 * </p>
 * <p>
 *     The certifications awarded to an asset can be captured in the metadata repository to enable both
 *     use and management of the certification process.
 * </p>
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class Certification extends Referenceable
{
    /*
     * Properties of a certification
     */
    private String            certificationTypeGUID   = null;
    private String            certificationTypeName   = null;
    private String            examiner                = null;
    private String            summary                 = null;
    private ExternalReference link                    = null;
    private Date              startDate               = null;
    private Date              endDate                 = null;
    private String            certificationConditions = null;
    private String            createdBy               = null;
    private String            custodian               = null;
    private String            notes                   = null;


    /**
     * Default constructor.
     */
    public Certification()
    {
        super();
    }


    /**
     * Copy/clone constructor.
     *
     * @param templateCertification - element to copy
     */
    public Certification(Certification templateCertification)
    {
        super(templateCertification);

        if (templateCertification != null)
        {
            certificationTypeGUID = templateCertification.getCertificationTypeGUID();
            certificationTypeName = templateCertification.getCertificationTypeName();
            examiner = templateCertification.getExaminer();
            summary = templateCertification.getSummary();

            ExternalReference  templateLink = templateCertification.getLink();
            if (templateLink != null)
            {
                link = new ExternalReference(templateLink);
            }

            Date               templateStartDate = templateCertification.getStartDate();
            if (templateStartDate != null)
            {
                startDate = new Date(templateStartDate.getTime());
            }

            Date               templateEndDate = templateCertification.getEndDate();
            if (templateEndDate != null)
            {
                endDate = new Date(templateStartDate.getTime());
            }

            certificationConditions = templateCertification.getCertificationConditions();
            createdBy = templateCertification.getCreatedBy();
            custodian = templateCertification.getCustodian();
            notes = templateCertification.getNotes();
        }
    }


    /**
     * Return the unique id for the type of certification.
     *
     * @return String certification type GUID
     */
    public String getCertificationTypeGUID() { return certificationTypeGUID; }


    /**
     * Set up the unique id of the certification type.
     *
     * @param certificationGUID - String certification type GUID
     */
    public void setCertificationTypeGUID(String certificationGUID) { this.certificationTypeGUID = certificationGUID; }


    /**
     * Return the type of the certification.
     *
     * @return String certification type
     */
    public String getCertificationTypeName() { return certificationTypeName; }


    /**
     * Set up the type of the certification.
     *
     * @param certificationTypeName - String certification type
     */
    public void setCertificationTypeName(String certificationTypeName) { this.certificationTypeName = certificationTypeName; }


    /**
     * Get the name of the organization or person that issued the certification.
     *
     * @return String name
     */
    public String getExaminer() { return examiner; }


    /**
     * Set up the name of the organization or person that issued the certification.
     *
     * @param examiner - String name
     */
    public void setExaminer(String examiner) { this.examiner = examiner; }


    /**
     * Return a brief summary of the certification.
     *
     * @return String summary
     */
    public String getSummary() { return summary; }


    /**
     * Set up a brief summary of the certification.
     *
     * @param summary - String
     */
    public void setSummary(String summary) { this.summary = summary; }


    /**
     * Return the link to the full text of the certification.
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
     * Set up the link to the full text of the certification.
     *
     * @param link - ExternalReference for full text
     */
    public void setLink(ExternalReference link) { this.link = link; }


    /**
     * Return the start date for the certification.  Null means unknown or not relevant.
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
     * Set up start date for the certification.  Null means unknown or not relevant.
     *
     * @param startDate - Date
     */
    public void setStartDate(Date startDate) { this.startDate = startDate; }


    /**
     * Return the end date for the certification.   Null means it does not expire.
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
     * Set up the end date for the certification.  Null means it does not expire.
     *
     * @param endDate - Date
     */
    public void setEndDate(Date endDate) { this.endDate = endDate; }


    /**
     * Return any special conditions that apply to the certification - such as endorsements.
     *
     * @return String certification conditions
     */
    public String getCertificationConditions() { return certificationConditions; }


    /**
     * Set up any special conditions that apply to the certification - such as endorsements.
     *
     * @param conditions - String certification conditions
     */
    public void setCertificationConditions(String conditions) { this.certificationConditions = conditions; }


    /**
     * Return the name of the person or organization that set up the certification for this asset.
     *
     * @return String name
     */
    public String getCreatedBy() { return createdBy; }


    /**
     * Set up the name of the person or organization that set up the certification for this asset.
     *
     * @param createdBy - String name
     */
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }


    /**
     * Return the name of the person or organization that is responsible for the correct management of the asset
     * according to the certification.
     *
     * @return String name
     */
    public String getCustodian() { return custodian; }


    /**
     * Set up the name of the person or organization that is responsible for the correct management of the asset
     * according to the certification.
     *
     * @param custodian - String name
     */
    public void setCustodian(String custodian) { this.custodian = custodian; }


    /**
     * Return the notes from the custodian.
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