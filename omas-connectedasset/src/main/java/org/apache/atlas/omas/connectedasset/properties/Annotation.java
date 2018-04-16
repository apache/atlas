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
 * An annotation describes the results of an analysis undertaken by an Open Discovery Framework (ODF) discovery service.
 * It describes when the analysis happened, the type of analysis and the results.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
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
    private String            annotationType = null;
    private String            summary = null;
    private int               confidenceLevel = 0;
    private String            expression = null;
    private String            explanation = null;
    private String            analysisStep = null;
    private String            jsonProperties = null;
    private AnnotationStatus  annotationStatus = null;

    /*
     * Details from the latest AnnotationReview entity.
     */
    private Date         reviewDate = null;
    private String       steward = null;
    private String       reviewComment = null;

    /*
     * Additional properties added directly to the Annotation entity and supported by
     * the sub-types of Annotation.
     */
    private   AdditionalProperties   additionalProperties = null;


    /**
     * Default Constructor
     */
    public Annotation()
    {
        super();
    }


    /**
     * Copy/clone Constructor
     *
     * @param templateAnnotation - template object to copy.
     */
    public Annotation(Annotation   templateAnnotation)
    {
        /*
         * Remember the parent
         */
        super(templateAnnotation);

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
            this.additionalProperties = new AdditionalProperties(templateAnnotation.getAdditionalProperties());
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
     * Set up the discovery analysis report name.
     *
     * @param reportName - report name string.
     */
    public void setReportName(String reportName)
    {
        this.reportName = reportName;
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
     * Annotations are created as part of a DiscoveryAnalysisReport.  This property contains the overall
     * report description associated with this annotation.
     *
     * @param reportDescription - String for the report description
     */
    public void setReportDescription(String reportDescription)
    {
        this.reportDescription = reportDescription;
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
     * Set up the creation date for the annotation.  This value should be available.  It is supplied on the
     * DiscoveryAnalysisReport entity.  However, if no creation data is available, this property is stored as null.
     *
     * @param creationDate - Date that annotation was created.
     */
    public void setCreationDate(Date creationDate)
    {
        this.creationDate = creationDate;
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
     * Set up the properties that hold the parameters used to drive the discovery service's analysis.
     *
     * @param analysisParameters - Properties for the analysis parameters
     */
    public void setAnalysisParameters(AdditionalProperties analysisParameters)
    {
        this.analysisParameters = analysisParameters;
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
     * Set up the informal name for the type of annotation.  The formal name in the model is given in the
     * ElementType.
     *
     * @param annotationType - String for annotation type
     */
    public void setAnnotationType(String annotationType)
    {
        this.annotationType = annotationType;
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
     * Set up the summary description of the annotation.
     *
     * @param summary - String for summary
     */
    public void setSummary(String summary)
    {
        this.summary = summary;
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
     * Set up the confidence level of the discovery service that the annotation is correct.
     *
     * @param confidenceLevel - int for confidence level
     */
    public void setConfidence(int confidenceLevel)
    {
        this.confidenceLevel = confidenceLevel;
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
     * Set up the expression that represent the relationship between the annotation and the asset.
     *
     * @param expression - string for expression
     */
    public void setExpression(String expression)
    {
        this.expression = expression;
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
     * Set up the explanation for the annotation.
     *
     * @param explanation - String for the explanation
     */
    public void setExplanation(String explanation)
    {
        this.explanation = explanation;
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
     * Set up the description of the analysis step that the discovery service was in when it created the annotation.
     *
     * @param analysisStep - String describing the analysis step
     */
    public void setAnalysisStep(String analysisStep)
    {
        this.analysisStep = analysisStep;
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
     * Set up the JSON properties associated with the annotation.
     *
     * @param jsonProperties String - JSON properties of annotation
     */
    public void setJsonProperties(String jsonProperties)
    {
        this.jsonProperties = jsonProperties;
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
     * Set up the current status of the annotation.
     *
     * @param annotationStatus - AnnotationStatus enum
     */
    public void setAnnotationStatus(AnnotationStatus annotationStatus)
    {
        this.annotationStatus = annotationStatus;
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
     * Set up the date that this annotation was reviewed.  If no review has taken place then this property is null.
     *
     * @param reviewDate - date review conducted
     */
    public void setReviewDate(Date reviewDate)
    {
        this.reviewDate = reviewDate;
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
     * Set up the name of the steward that reviewed the annotation.
     *
     * @param steward String name of steward that reviewed the annotation.
     */
    public void setSteward(String steward)
    {
        this.steward = steward;
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
     * Set the comment made by the steward as part of the review of the annotation. The comment covers the
     * whole review which may have looked at multiple annotations so the comment may not necessarily
     * refer to this annotation.
     *
     * @param reviewComment - string comment
     */
    public void setReviewComment(String reviewComment)
    {
        this.reviewComment = reviewComment;
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
     * Set up the additional properties for the annotation.  These are a combination of the additional
     * properties from the Annotation entity and any properties introduced by the subtypes of annotation.
     * The naming convention for subtype property names is entityName.attributeName.value.  If the property
     * is a map then the map contents are named entityName.attributeName.propertyName.propertyValue.
     *
     * @param additionalProperties - additional properties object for annotation.
     */
    public void setAdditionalProperties(AdditionalProperties additionalProperties)
    {
        this.additionalProperties = additionalProperties;
    }
}