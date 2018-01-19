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
 * An annotation describes the results of an analysis undertaken by an Open Discovery Framework (ODF) discovery service.
 * It describes when the analysis happened, the type of analysis and the results.
 */
public class Annotation extends ElementHeader
{
    /*
     * Details from the AnnotationReport entity
     */
    private String               reportName = null;
    private String               reportDescription = null;
    private Date                 creationDate = null;
    private AdditionalProperties analysisParameters = null;

    /*
     * Details from the Annotation entity itself
     */
    private String               annotationType = null;
    private String               summary = null;
    private int                  confidenceLevel = 0;
    private String               expression = null;
    private String               explanation = null;
    private String               analysisStep = null;
    private String               jsonProperties = null;
    private AnnotationStatus     annotationStatus = null;

    /*
     * Details from the latest AnnotationReview entity.
     */
    private Date                 reviewDate = null;
    private String               steward = null;
    private String               reviewComment = null;

    /*
     * Additional properties added directly to the Annotation entity and supported by
     * the sub-types of Annotation.
     */
    private AdditionalProperties  additionalProperties = null;


    /**
     * Typical Constructor
     *
     * @param parentAsset - description of the asset that this annotation is attached to.
     * @param type - details of the metadata type for this properties object
     * @param guid - String - unique id
     * @param url - String - URL
     * @param classifications - List of classifications
     * @param reportName - report name string
     * @param reportDescription - String for the report description
     * @param creationDate - Date that annotation was created.
     * @param analysisParameters - Properties that hold the parameters used to drive the discovery service's analysis.
     * @param annotationType - String for annotation type
     * @param summary - String for summary
     * @param confidenceLevel - int for confidence level
     * @param expression - String for the expression that represent the relationship between the annotation and the asset.
     * @param explanation - String for the explanation for the annotation
     * @param analysisStep - String describing the analysis step that the discovery service was in when it created the annotation.
     * @param jsonProperties String - JSON properties associated with the annotation
     * @param annotationStatus - AnnotationStatus enum
     * @param reviewDate - date that this annotation was reviewed.  If no review has taken place then this property is null.
     * @param steward String name of steward that reviewed the annotation.
     * @param reviewComment - string comment made by the steward as part of the review of the annotation. The comment covers the
     * whole review which may have looked at multiple annotations so the comment may not necessarily refer only to this annotation.
     * @param additionalProperties - additional properties object for annotation.  These are a combination of the additional
     * properties from the Annotation entity and any properties introduced by the subtypes of annotation.
     * The naming convention for subtype property names is entityName.attributeName.value.  If the property
     * is a map then the map contents are named entityName.attributeName.propertyName.propertyValue.
     */


    public Annotation(AssetDescriptor       parentAsset,
                      ElementType           type,
                      String                guid,
                      String                url,
                      Classifications       classifications,
                      String                reportName,
                      String                reportDescription,
                      Date                  creationDate,
                      AdditionalProperties  analysisParameters,
                      String                annotationType,
                      String                summary,
                      int                   confidenceLevel,
                      String                expression,
                      String                explanation,
                      String                analysisStep,
                      String                jsonProperties,
                      AnnotationStatus      annotationStatus,
                      Date                  reviewDate,
                      String                steward,
                      String                reviewComment,
                      AdditionalProperties  additionalProperties)
    {
        super(parentAsset, type, guid, url, classifications);

        this.reportName = reportName;
        this.reportDescription = reportDescription;
        this.creationDate = creationDate;
        this.analysisParameters = analysisParameters;
        this.annotationType = annotationType;
        this.summary = summary;
        this.confidenceLevel = confidenceLevel;
        this.expression = expression;
        this.explanation = explanation;
        this.analysisStep = analysisStep;
        this.jsonProperties = jsonProperties;
        this.annotationStatus = annotationStatus;
        this.reviewDate = reviewDate;
        this.steward = steward;
        this.reviewComment = reviewComment;
        this.additionalProperties = additionalProperties;
    }

    /**
     * Copy/clone Constructor
     *
     * @param parentAsset - description of the asset that this annotation is attached to.
     * @param templateAnnotation - template object to copy.
     */
    public Annotation(AssetDescriptor    parentAsset, Annotation   templateAnnotation)
    {
        /*
         * Remember the parent
         */
        super(parentAsset, templateAnnotation);

        if (templateAnnotation != null)
        {
            /*
             * Copy the properties from the template into this annotation.
             */
            this.reportName = templateAnnotation.getReportName();
            this.reportDescription = templateAnnotation.getReportDescription();
            this.creationDate = templateAnnotation.getCreationDate();
            this.analysisParameters = templateAnnotation.getAnalysisParameters();
            this.annotationType = templateAnnotation.getAnnotationType();
            this.summary = templateAnnotation.getSummary();
            this.confidenceLevel = templateAnnotation.getConfidenceLevel();
            this.expression = templateAnnotation.getExpression();
            this.explanation = templateAnnotation.getExplanation();
            this.analysisStep = templateAnnotation.getAnalysisStep();
            this.jsonProperties = templateAnnotation.getJsonProperties();
            this.annotationStatus = templateAnnotation.getAnnotationStatus();
            this.reviewDate = templateAnnotation.getReviewDate();
            this.steward = templateAnnotation.getSteward();
            this.reviewComment = templateAnnotation.getReviewComment();
            this.additionalProperties = new AdditionalProperties(parentAsset, templateAnnotation.getAdditionalProperties());
        }
    }


    /**
     * Return the name of the discovery analysis report that created this annotation.
     *
     * @return String - report name
     */
    public String getReportName()
    {
        return reportName;
    }


    /**
     * Return the discovery analysis report description that this annotation is a part of.
     *
     * @return String - report description
     */
    public String getReportDescription()
    {
        return reportDescription;
    }


    /**
     * Return the creation date for the annotation.  If this date is not known then null is returned.
     *
     * @return Date that the annotation was created.
     */
    public Date getCreationDate() {
        return creationDate;
    }


    /**
     * Return the properties that hold the parameters used to drive the discovery service's analysis.
     *
     * @return AdditionalProperties - object storing the analysis parameters
     */
    public AdditionalProperties getAnalysisParameters()
    {
        return analysisParameters;
    }


    /**
     * Return the informal name for the type of annotation.
     *
     * @return String - annotation type
     */
    public String getAnnotationType()
    {
        return annotationType;
    }


    /**
     * Return the summary description for the annotation.
     *
     * @return String - summary of annotation
     */
    public String getSummary()
    {
        return summary;
    }


    /**
     * Return the confidence level of the discovery service that the annotation is correct.
     *
     * @return int - confidence level
     */
    public int getConfidenceLevel()
    {
        return confidenceLevel;
    }


    /**
     * Return the expression that represent the relationship between the annotation and the asset.
     *
     * @return String - expression
     */
    public String getExpression()
    {
        return expression;
    }


    /**
     * Return the explanation for the annotation.
     *
     * @return String - explanation
     */
    public String getExplanation() {
        return explanation;
    }


    /**
     * Return a description of the analysis step that the discovery service was in when it created the annotation.
     *
     * @return String - analysis step
     */
    public String getAnalysisStep()
    {
        return analysisStep;
    }


    /**
     * Return the JSON properties associated with the annotation.
     *
     * @return String - JSON properties of annotation
     */
    public String getJsonProperties()
    {
        return jsonProperties;
    }


    /**
     * Return the current status of the annotation.
     *
     * @return AnnotationStatus - current status of annotation
     */
    public AnnotationStatus getAnnotationStatus()
    {
        return annotationStatus;
    }


    /**
     * Return the date that this annotation was reviewed.  If no review has taken place then this property is null.
     *
     * @return Date - review date
     */
    public Date getReviewDate()
    {
        return reviewDate;
    }


    /**
     * Return the name of the steward that reviewed the annotation.
     *
     * @return String - steward's name.
     */
    public String getSteward()
    {
        return steward;
    }


    /**
     * Return any comments made by the steward during the review.
     *
     * @return String - review comment
     */
    public String getReviewComment()
    {
        return reviewComment;
    }


    /**
     * Return the additional properties for the Annotation.
     *
     * @return AdditionalProperties - additional properties object
     */
    public AdditionalProperties getAdditionalProperties()
    {
        return additionalProperties;
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "Annotation{" +
                "reportName='" + reportName + '\'' +
                ", reportDescription='" + reportDescription + '\'' +
                ", creationDate=" + creationDate +
                ", analysisParameters=" + analysisParameters +
                ", annotationType='" + annotationType + '\'' +
                ", summary='" + summary + '\'' +
                ", confidenceLevel=" + confidenceLevel +
                ", expression='" + expression + '\'' +
                ", explanation='" + explanation + '\'' +
                ", analysisStep='" + analysisStep + '\'' +
                ", jsonProperties='" + jsonProperties + '\'' +
                ", annotationStatus=" + annotationStatus +
                ", reviewDate=" + reviewDate +
                ", steward='" + steward + '\'' +
                ", reviewComment='" + reviewComment + '\'' +
                ", additionalProperties=" + additionalProperties +
                ", type=" + type +
                ", guid='" + guid + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}