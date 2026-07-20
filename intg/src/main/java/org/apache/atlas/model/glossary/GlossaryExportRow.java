/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.model.glossary;

import org.apache.atlas.model.annotation.AtlasJSON;

import java.io.Serializable;

@AtlasJSON
public class GlossaryExportRow implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum RecordTypeKind {
        TERM, CATEGORY
    }

    private RecordTypeKind recordType;
    private String         guid;
    private String         glossaryGuid;
    private String         name;
    private String         glossaryName;
    private String         shortDescription;
    private String         longDescription;
    private String         status;
    private String         classifications;
    private String         customAttributes;
    private String         relatedCategoriesOrParent;
    private String         qualifiedName;
    private String         entityStatus;

    // Import-compatible relationship columns (terms only)
    private String examples;
    private String abbreviation;
    private String usage;
    private String translationTerms;
    private String validValuesFor;
    private String synonyms;
    private String replacedBy;
    private String validValues;
    private String replacementTerms;
    private String seeAlso;
    private String translatedTerms;
    private String isA;
    private String antonyms;
    private String classifies;
    private String preferredToTerms;
    private String preferredTerms;

    public RecordTypeKind getRecordType() {
        return recordType;
    }

    public void setRecordType(RecordTypeKind recordType) {
        this.recordType = recordType;
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public String getGlossaryGuid() {
        return glossaryGuid;
    }

    public void setGlossaryGuid(String glossaryGuid) {
        this.glossaryGuid = glossaryGuid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGlossaryName() {
        return glossaryName;
    }

    public void setGlossaryName(String glossaryName) {
        this.glossaryName = glossaryName;
    }

    public String getShortDescription() {
        return shortDescription;
    }

    public void setShortDescription(String shortDescription) {
        this.shortDescription = shortDescription;
    }

    public String getLongDescription() {
        return longDescription;
    }

    public void setLongDescription(String longDescription) {
        this.longDescription = longDescription;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getClassifications() {
        return classifications;
    }

    public void setClassifications(String classifications) {
        this.classifications = classifications;
    }

    public String getCustomAttributes() {
        return customAttributes;
    }

    public void setCustomAttributes(String customAttributes) {
        this.customAttributes = customAttributes;
    }

    public String getRelatedCategoriesOrParent() {
        return relatedCategoriesOrParent;
    }

    public void setRelatedCategoriesOrParent(String relatedCategoriesOrParent) {
        this.relatedCategoriesOrParent = relatedCategoriesOrParent;
    }

    public String getQualifiedName() {
        return qualifiedName;
    }

    public void setQualifiedName(String qualifiedName) {
        this.qualifiedName = qualifiedName;
    }

    public String getEntityStatus() {
        return entityStatus;
    }

    public void setEntityStatus(String entityStatus) {
        this.entityStatus = entityStatus;
    }

    public String getExamples() {
        return examples;
    }

    public void setExamples(String examples) {
        this.examples = examples;
    }

    public String getAbbreviation() {
        return abbreviation;
    }

    public void setAbbreviation(String abbreviation) {
        this.abbreviation = abbreviation;
    }

    public String getUsage() {
        return usage;
    }

    public void setUsage(String usage) {
        this.usage = usage;
    }

    public String getTranslationTerms() {
        return translationTerms;
    }

    public void setTranslationTerms(String translationTerms) {
        this.translationTerms = translationTerms;
    }

    public String getValidValuesFor() {
        return validValuesFor;
    }

    public void setValidValuesFor(String validValuesFor) {
        this.validValuesFor = validValuesFor;
    }

    public String getSynonyms() {
        return synonyms;
    }

    public void setSynonyms(String synonyms) {
        this.synonyms = synonyms;
    }

    public String getReplacedBy() {
        return replacedBy;
    }

    public void setReplacedBy(String replacedBy) {
        this.replacedBy = replacedBy;
    }

    public String getValidValues() {
        return validValues;
    }

    public void setValidValues(String validValues) {
        this.validValues = validValues;
    }

    public String getReplacementTerms() {
        return replacementTerms;
    }

    public void setReplacementTerms(String replacementTerms) {
        this.replacementTerms = replacementTerms;
    }

    public String getSeeAlso() {
        return seeAlso;
    }

    public void setSeeAlso(String seeAlso) {
        this.seeAlso = seeAlso;
    }

    public String getTranslatedTerms() {
        return translatedTerms;
    }

    public void setTranslatedTerms(String translatedTerms) {
        this.translatedTerms = translatedTerms;
    }

    public String getIsA() {
        return isA;
    }

    public void setIsA(String isA) {
        this.isA = isA;
    }

    public String getAntonyms() {
        return antonyms;
    }

    public void setAntonyms(String antonyms) {
        this.antonyms = antonyms;
    }

    public String getClassifies() {
        return classifies;
    }

    public void setClassifies(String classifies) {
        this.classifies = classifies;
    }

    public String getPreferredToTerms() {
        return preferredToTerms;
    }

    public void setPreferredToTerms(String preferredToTerms) {
        this.preferredToTerms = preferredToTerms;
    }

    public String getPreferredTerms() {
        return preferredTerms;
    }

    public void setPreferredTerms(String preferredTerms) {
        this.preferredTerms = preferredTerms;
    }
}
