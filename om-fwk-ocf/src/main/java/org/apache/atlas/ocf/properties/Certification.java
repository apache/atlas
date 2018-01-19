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
 *     Certification stores the certifications awarded to the asset.
 * </p>
 * <p>
 *     Many regulations and industry bodies define certifications that can confirm a level of support,
 *     capability or competence in an aspect of a digital organization’s operation.
 *     Having certifications may be necessary to operating legally or may be a business advantage.
 * </p>
 * <p>
 *     The certifications awarded to an asset can be captured in the metadata repository to enable both
 *     effective use and management of the certification process.
 * </p>
 */
public class Certification extends Referenceable
{
    /*
     * Properties of a certification
     */
    private  String                 certificationTypeGUID = null;
    private  String                 certificationTypeName = null;
    private  String                 examiner = null;
    private  String                 summary = null;
    private  ExternalReference      link = null;
    private  Date                   startDate = null;
    private  Date                   endDate = null;
    private  String                 certificationConditions = null;
    private  String                 createdBy = null;
    private  String                 custodian = null;
    private  String                 notes = null;


    /**
     * Typical constructor.
     *
     * @param parentAsset - descriptor for parent asset
     * @param type - details of the metadata type for this properties object
     * @param guid - unique id
     * @param url - URL of the certification in the metadata repository
     * @param classifications - enumeration of classifications
     * @param qualifiedName - unique name
     * @param additionalProperties - additional properties for the referenceable object
     * @param meanings - list of glossary terms (summary)
     * @param certificationTypeGUID - certification type GUID
     * @param certificationTypeName - certification type name
     * @param examiner - name of the organization or person that issued the certification
     * @param summary - brief summary of the certification
     * @param link - external reference for full text of the certification.
     * @param startDate - start date for the certification.  Null means unknown or not relevant.
     * @param endDate - end date for the certification.  Null means it does not expire.
     * @param certificationConditions - any special conditions that apply to the certification - such as endorsements
     * @param createdBy - name of the person or organization that set up the certification for this asset
     * @param custodian - String name of the person or organization that is responsible for the correct management
     *                  of the asset according to the certification
     * @param notes - String notes from the custodian
     */
    public Certification(AssetDescriptor      parentAsset,
                         ElementType          type,
                         String               guid,
                         String               url,
                         Classifications      classifications,
                         String               qualifiedName,
                         AdditionalProperties additionalProperties,
                         Meanings             meanings,
                         String               certificationTypeGUID,
                         String               certificationTypeName,
                         String               examiner,
                         String               summary,
                         ExternalReference    link,
                         Date                 startDate,
                         Date                 endDate,
                         String               certificationConditions,
                         String               createdBy,
                         String               custodian,
                         String               notes)
    {
        super(parentAsset, type, guid, url, classifications, qualifiedName, additionalProperties, meanings);

        this.certificationTypeGUID = certificationTypeGUID;
        this.certificationTypeName = certificationTypeName;
        this.examiner = examiner;
        this.summary = summary;
        this.link = link;
        this.startDate = startDate;
        this.endDate = endDate;
        this.certificationConditions = certificationConditions;
        this.createdBy = createdBy;
        this.custodian = custodian;
        this.notes = notes;
    }

    /**
     * Copy/clone constructor.
     *
     * @param parentAsset - descriptor for parent asset
     * @param templateCertification - element to copy
     */
    public Certification(AssetDescriptor   parentAsset, Certification templateCertification)
    {
        super(parentAsset, templateCertification);

        if (templateCertification != null)
        {
            certificationTypeGUID = templateCertification.getCertificationTypeGUID();
            certificationTypeName = templateCertification.getCertificationTypeName();
            examiner = templateCertification.getExaminer();
            summary = templateCertification.getSummary();

            ExternalReference  templateLink = templateCertification.getLink();
            if (templateLink != null)
            {
                link = new ExternalReference(parentAsset, templateLink);
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
     * Return the type of the certification.
     *
     * @return String certification type
     */
    public String getCertificationTypeName() { return certificationTypeName; }


    /**
     * Return the name of the organization or person that issued the certification.
     *
     * @return String name
     */
    public String getExaminer() { return examiner; }


    /**
     * Return a brief summary of the certification.
     *
     * @return String summary
     */
    public String getSummary() { return summary; }


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
            return new ExternalReference(super.getParentAsset(), link);
        }
    }


    /**
     * Return the start date for the certification.  Null means unknown or not relevant.
     *
     * @return Date - start date for the certification
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
     * Return the end date for the certification.   Null means it does not expire.
     *
     * @return Date - end date for the certification
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
     * Return any special conditions that apply to the certification - such as endorsements.
     *
     * @return String certification conditions
     */
    public String getCertificationConditions() { return certificationConditions; }


    /**
     * Return the name of the person or organization that set up the certification for this asset.
     *
     * @return String name
     */
    public String getCreatedBy() { return createdBy; }


    /**
     * Return the name of the person or organization that is responsible for the correct management of the asset
     * according to the certification.
     *
     * @return String name
     */
    public String getCustodian() { return custodian; }


    /**
     * Return the notes from the custodian.
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
        return "Certification{" +
                "certificationTypeGUID='" + certificationTypeGUID + '\'' +
                ", certificationTypeName='" + certificationTypeName + '\'' +
                ", examiner='" + examiner + '\'' +
                ", summary='" + summary + '\'' +
                ", link=" + link +
                ", startDate=" + startDate +
                ", endDate=" + endDate +
                ", certificationConditions='" + certificationConditions + '\'' +
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