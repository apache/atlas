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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.atlas.model.annotation.AtlasJSON;
import org.apache.atlas.model.glossary.relations.AtlasGlossaryHeader;
import org.apache.atlas.model.glossary.relations.AtlasRelatedTermHeader;
import org.apache.atlas.model.glossary.relations.AtlasTermCategorizationHeader;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@AtlasJSON
public class AtlasGlossaryTerm extends AtlasGlossaryBaseObject {
    // Core attributes
    private List<String> examples;
    private String       abbreviation;
    private String       usage;

    // Attributes derived from relationships
    private AtlasGlossaryHeader                anchor;
    private Set<AtlasEntityHeader>             assignedEntities;
    private Set<AtlasTermCategorizationHeader> categories;

    // Related Terms
    private Set<AtlasRelatedTermHeader> seeAlso;

    // Term Synonyms
    private Set<AtlasRelatedTermHeader> synonyms;

    // Term antonyms
    private Set<AtlasRelatedTermHeader> antonyms;

    // Term preference
    private Set<AtlasRelatedTermHeader> preferredTerms;
    private Set<AtlasRelatedTermHeader> preferredToTerms;

    // Term replacements
    private Set<AtlasRelatedTermHeader> replacementTerms;
    private Set<AtlasRelatedTermHeader> replacedBy;

    // Term translations
    private Set<AtlasRelatedTermHeader> translationTerms;
    private Set<AtlasRelatedTermHeader> translatedTerms;

    // Term classification
    private Set<AtlasRelatedTermHeader> isA;
    private Set<AtlasRelatedTermHeader> classifies;

    // Values for terms
    private Set<AtlasRelatedTermHeader> validValues;
    private Set<AtlasRelatedTermHeader> validValuesFor;

    private boolean hasTerms = false;

    public AtlasGlossaryTerm() {
    }

    public List<String> getExamples() {
        return examples;
    }

    public void setExamples(final List<String> examples) {
        this.examples = examples;
    }

    public String getAbbreviation() {
        return abbreviation;
    }

    public void setAbbreviation(final String abbreviation) {
        this.abbreviation = abbreviation;
    }

    public String getUsage() {
        return usage;
    }

    public void setUsage(final String usage) {
        this.usage = usage;
    }

    public AtlasGlossaryHeader getAnchor() {
        return anchor;
    }

    public void setAnchor(final AtlasGlossaryHeader anchor) {
        this.anchor = anchor;
    }

    public Set<AtlasTermCategorizationHeader> getCategories() {
        return categories;
    }

    public void setCategories(final Set<AtlasTermCategorizationHeader> categories) {
        this.categories = categories;
    }

    public void addCategory(final AtlasTermCategorizationHeader category) {
        Set<AtlasTermCategorizationHeader> categories = this.categories;
        if (categories == null) {
            categories = new HashSet<>();
        }
        categories.add(category);
        setCategories(categories);
    }

    public Set<AtlasEntityHeader> getAssignedEntities() {
        return assignedEntities;
    }

    public void setAssignedEntities(final Set<AtlasEntityHeader> assignedEntities) {
        this.assignedEntities = assignedEntities;
    }

    public void addAssignedEntity(final AtlasEntityHeader entityHeader) {
        Set<AtlasEntityHeader> entityHeaders = this.assignedEntities;
        if (entityHeaders == null) {
            entityHeaders = new HashSet<>();
        }
        entityHeaders.add(entityHeader);
        setAssignedEntities(entityHeaders);
    }

    public Set<AtlasRelatedTermHeader> getSeeAlso() {
        return seeAlso;
    }

    public void setSeeAlso(final Set<AtlasRelatedTermHeader> seeAlso) {
        this.seeAlso = seeAlso;
        hasTerms = true;
    }

    public Set<AtlasRelatedTermHeader> getSynonyms() {
        return synonyms;
    }

    public void setSynonyms(final Set<AtlasRelatedTermHeader> synonyms) {
        this.synonyms = synonyms;
        hasTerms = true;
    }

    public Set<AtlasRelatedTermHeader> getAntonyms() {
        return antonyms;
    }

    public void setAntonyms(final Set<AtlasRelatedTermHeader> antonyms) {
        this.antonyms = antonyms;
        hasTerms = true;
    }

    public Set<AtlasRelatedTermHeader> getPreferredTerms() {
        return preferredTerms;
    }

    public void setPreferredTerms(final Set<AtlasRelatedTermHeader> preferredTerms) {
        this.preferredTerms = preferredTerms;
        hasTerms = true;
    }

    public Set<AtlasRelatedTermHeader> getPreferredToTerms() {
        return preferredToTerms;
    }

    public void setPreferredToTerms(final Set<AtlasRelatedTermHeader> preferredToTerms) {
        this.preferredToTerms = preferredToTerms;
    }

    public Set<AtlasRelatedTermHeader> getReplacementTerms() {
        return replacementTerms;
    }

    public void setReplacementTerms(final Set<AtlasRelatedTermHeader> replacementTerms) {
        this.replacementTerms = replacementTerms;
        hasTerms = true;
    }

    public Set<AtlasRelatedTermHeader> getReplacedBy() {
        return replacedBy;
    }

    public void setReplacedBy(final Set<AtlasRelatedTermHeader> replacedBy) {
        this.replacedBy = replacedBy;
        hasTerms = true;
    }

    public Set<AtlasRelatedTermHeader> getTranslationTerms() {
        return translationTerms;
    }

    public void setTranslationTerms(final Set<AtlasRelatedTermHeader> translationTerms) {
        this.translationTerms = translationTerms;
        hasTerms = true;
    }

    public Set<AtlasRelatedTermHeader> getTranslatedTerms() {
        return translatedTerms;
    }

    public void setTranslatedTerms(final Set<AtlasRelatedTermHeader> translatedTerms) {
        this.translatedTerms = translatedTerms;
        hasTerms = true;
    }

    public Set<AtlasRelatedTermHeader> getIsA() {
        return isA;
    }

    public void setIsA(final Set<AtlasRelatedTermHeader> isA) {
        this.isA = isA;
        hasTerms = true;
    }

    public Set<AtlasRelatedTermHeader> getClassifies() {
        return classifies;
    }

    public void setClassifies(final Set<AtlasRelatedTermHeader> classifies) {
        this.classifies = classifies;
        hasTerms = true;
    }

    public Set<AtlasRelatedTermHeader> getValidValues() {
        return validValues;
    }

    public void setValidValues(final Set<AtlasRelatedTermHeader> validValues) {
        this.validValues = validValues;
        hasTerms = true;
    }

    public Set<AtlasRelatedTermHeader> getValidValuesFor() {
        return validValuesFor;
    }

    public void setValidValuesFor(final Set<AtlasRelatedTermHeader> validValuesFor) {
        this.validValuesFor = validValuesFor;
        hasTerms = true;
    }

    @JsonIgnore
    public boolean hasTerms() {
        return hasTerms;
    }

    @JsonIgnore
    @Override
    public void setAttribute(String attrName, String attrVal) {
        Objects.requireNonNull(attrName, "AtlasGlossaryTerm attribute name");
        switch (attrName) {
            case "displayName":
                setDisplayName(attrVal);
                break;
            case "shortDescription":
                setShortDescription(attrVal);
                break;
            case "longDescription":
                setLongDescription(attrVal);
                break;
            case "abbreviation":
                setAbbreviation(attrVal);
                break;
            case "usage":
                setUsage(attrVal);
                break;
            default:
                throw new IllegalArgumentException("Invalid attribute '" + attrName + "' for object AtlasGlossaryTerm");
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AtlasGlossaryTerm{");
        sb.append("examples=").append(examples);
        sb.append(", abbreviation='").append(abbreviation).append('\'');
        sb.append(", usage='").append(usage).append('\'');
        sb.append(", anchor=").append(anchor);
        sb.append(", assignedEntities=").append(assignedEntities);
        sb.append(", categories=").append(categories);
        sb.append(", seeAlso=").append(seeAlso);
        sb.append(", synonyms=").append(synonyms);
        sb.append(", antonyms=").append(antonyms);
        sb.append(", preferredTerms=").append(preferredTerms);
        sb.append(", preferredToTerms=").append(preferredToTerms);
        sb.append(", replacementTerms=").append(replacementTerms);
        sb.append(", replacedBy=").append(replacedBy);
        sb.append(", translationTerms=").append(translationTerms);
        sb.append(", translatedTerms=").append(translatedTerms);
        sb.append(", isA=").append(isA);
        sb.append(", classifies=").append(classifies);
        sb.append(", validValues=").append(validValues);
        sb.append(", validValuesFor=").append(validValuesFor);
        sb.append(", hasTerms=").append(hasTerms);
        sb.append('}');
        return sb.toString();
    }

    @JsonIgnore
    public Map<Relation, Set<AtlasRelatedTermHeader>> getRelatedTerms() {
        Map<Relation, Set<AtlasRelatedTermHeader>> ret = new HashMap<>();

        if (CollectionUtils.isNotEmpty(seeAlso)) {
            ret.put(Relation.SEE_ALSO, seeAlso);
        }

        if (CollectionUtils.isNotEmpty(synonyms)) {
            ret.put(Relation.SYNONYMS, synonyms);
        }

        if (CollectionUtils.isNotEmpty(antonyms)) {
            ret.put(Relation.ANTONYMS, antonyms);
        }

        if (CollectionUtils.isNotEmpty(preferredTerms)) {
            ret.put(Relation.PREFERRED_TERMS, preferredTerms);
        }

        if (CollectionUtils.isNotEmpty(preferredToTerms)) {
            ret.put(Relation.PREFERRED_TO_TERMS, preferredToTerms);
        }

        if (CollectionUtils.isNotEmpty(replacementTerms)) {
            ret.put(Relation.REPLACEMENT_TERMS, replacementTerms);
        }

        if (CollectionUtils.isNotEmpty(replacedBy)) {
            ret.put(Relation.REPLACED_BY, replacedBy);
        }

        if (CollectionUtils.isNotEmpty(translationTerms)) {
            ret.put(Relation.TRANSLATION_TERMS, translationTerms);
        }

        if (CollectionUtils.isNotEmpty(translatedTerms)) {
            ret.put(Relation.TRANSLATED_TERMS, translatedTerms);
        }

        if (CollectionUtils.isNotEmpty(isA)) {
            ret.put(Relation.ISA, isA);
        }

        if (CollectionUtils.isNotEmpty(classifies)) {
            ret.put(Relation.CLASSIFIES, classifies);
        }

        if (CollectionUtils.isNotEmpty(validValues)) {
            ret.put(Relation.VALID_VALUES, validValues);
        }

        if (CollectionUtils.isNotEmpty(validValuesFor)) {
            ret.put(Relation.VALID_VALUES_FOR, validValuesFor);
        }

        return ret;
    }

    @Override
    protected StringBuilder toString(final StringBuilder sb) {
        return sb == null ? new StringBuilder(toString()) : sb.append(toString());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof AtlasGlossaryTerm)) return false;
        if (!super.equals(o)) return false;
        final AtlasGlossaryTerm that = (AtlasGlossaryTerm) o;
        return Objects.equals(examples, that.examples) &&
                       Objects.equals(abbreviation, that.abbreviation) &&
                       Objects.equals(usage, that.usage) &&
                       Objects.equals(anchor, that.anchor) &&
                       Objects.equals(assignedEntities, that.assignedEntities) &&
                       Objects.equals(categories, that.categories) &&
                       Objects.equals(seeAlso, that.seeAlso) &&
                       Objects.equals(synonyms, that.synonyms) &&
                       Objects.equals(antonyms, that.antonyms) &&
                       Objects.equals(preferredTerms, that.preferredTerms) &&
                       Objects.equals(preferredToTerms, that.preferredToTerms) &&
                       Objects.equals(replacementTerms, that.replacementTerms) &&
                       Objects.equals(replacedBy, that.replacedBy) &&
                       Objects.equals(translationTerms, that.translationTerms) &&
                       Objects.equals(translatedTerms, that.translatedTerms) &&
                       Objects.equals(isA, that.isA) &&
                       Objects.equals(classifies, that.classifies) &&
                       Objects.equals(validValues, that.validValues) &&
                       Objects.equals(validValuesFor, that.validValuesFor);
    }

    @Override
    public int hashCode() {

        return Objects.hash(super.hashCode(), examples, abbreviation, usage, anchor, assignedEntities, categories,
                            seeAlso, synonyms, antonyms, preferredTerms, preferredToTerms, replacementTerms, replacedBy,
                            translationTerms, translatedTerms, isA, classifies, validValues, validValuesFor);
    }

    public enum Relation {
        SEE_ALSO("__AtlasGlossaryRelatedTerm", "seeAlso"),
        SYNONYMS("__AtlasGlossarySynonym", "synonyms"),
        ANTONYMS("__AtlasGlossaryAntonym", "antonyms"),
        PREFERRED_TERMS("__AtlasGlossaryPreferredTerm", "preferredTerms"),
        PREFERRED_TO_TERMS("__AtlasGlossaryPreferredTerm", "preferredToTerms"),
        REPLACEMENT_TERMS("__AtlasGlossaryReplacementTerm", "replacementTerms"),
        REPLACED_BY("__AtlasGlossaryReplacementTerm", "replacedBy"),
        TRANSLATION_TERMS("__AtlasGlossaryTranslation", "translationTerms"),
        TRANSLATED_TERMS("__AtlasGlossaryTranslation", "translatedTerms"),
        ISA("__AtlasGlossaryIsARelationship", "isA"),
        CLASSIFIES("__AtlasGlossaryIsARelationship", "classifies"),
        VALID_VALUES("__AtlasGlossaryValidValue", "validValues"),
        VALID_VALUES_FOR("__AtlasGlossaryValidValue", "validValuesFor"),
        ;

        private String relationName;
        private String relationAttrName;

        Relation(final String relationName, final String relationAttrName) {
            this.relationName = relationName;
            this.relationAttrName = relationAttrName;
        }

        public String getRelationName() {
            return relationName;
        }

        @JsonValue
        public String getRelationAttrName() {
            return relationAttrName;
        }
    }
}
