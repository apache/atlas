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
import org.apache.atlas.model.instance.AtlasClassification;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Response for glossary search (display API). Nested glossary → terms/categories structure for the UI table.
 */
@AtlasJSON
public class GlossarySearchResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private List<GlossaryDetail> glossary         = new ArrayList<>();
    private long                 approximateCount;

    public List<GlossaryDetail> getGlossary() {
        if (glossary == null) {
            glossary = new ArrayList<>();
        }

        return glossary;
    }

    public void setGlossary(List<GlossaryDetail> glossary) {
        this.glossary = glossary != null ? glossary : new ArrayList<>();
    }

    public long getApproximateCount() {
        return approximateCount;
    }

    public void setApproximateCount(long approximateCount) {
        this.approximateCount = approximateCount;
    }

    @AtlasJSON
    public static class GlossaryDetail implements Serializable {
        private static final long serialVersionUID = 1L;

        private String                      name;
        private String                      guid;
        private String                      shortDescription;
        private String                      longDescription;
        private String                      status;
        private List<GlossaryTermDetail>     terms      = new ArrayList<>();
        private List<GlossaryCategoryDetail> categories = new ArrayList<>();

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getGuid() {
            return guid;
        }

        public void setGuid(String guid) {
            this.guid = guid;
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

        public List<GlossaryTermDetail> getTerms() {
            if (terms == null) {
                terms = new ArrayList<>();
            }

            return terms;
        }

        public void setTerms(List<GlossaryTermDetail> terms) {
            this.terms = terms != null ? terms : new ArrayList<>();
        }

        public List<GlossaryCategoryDetail> getCategories() {
            if (categories == null) {
                categories = new ArrayList<>();
            }

            return categories;
        }

        public void setCategories(List<GlossaryCategoryDetail> categories) {
            this.categories = categories != null ? categories : new ArrayList<>();
        }
    }

    @AtlasJSON
    public static class GlossaryTermDetail implements Serializable {
        private static final long serialVersionUID = 1L;

        private String                   name;
        private String                   qualifiedName;
        private String                   guid;
        private String                   shortDescription;
        private String                   longDescription;
        private String                   status;
        private List<AtlasClassification> classifications = new ArrayList<>();
        private Map<String, Object>      customAttributes;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getQualifiedName() {
            return qualifiedName;
        }

        public void setQualifiedName(String qualifiedName) {
            this.qualifiedName = qualifiedName;
        }

        public String getGuid() {
            return guid;
        }

        public void setGuid(String guid) {
            this.guid = guid;
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

        public List<AtlasClassification> getClassifications() {
            return classifications;
        }

        public void setClassifications(List<AtlasClassification> classifications) {
            this.classifications = classifications;
        }

        public Map<String, Object> getCustomAttributes() {
            return customAttributes;
        }

        public void setCustomAttributes(Map<String, Object> customAttributes) {
            this.customAttributes = customAttributes;
        }
    }

    @AtlasJSON
    public static class GlossaryCategoryDetail implements Serializable {
        private static final long serialVersionUID = 1L;

        private String                   name;
        private String                   qualifiedName;
        private String                   guid;
        private String                   shortDescription;
        private String                   longDescription;
        private String                   status;
        private List<AtlasClassification> classifications = new ArrayList<>();
        private Map<String, Object>      customAttributes;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getQualifiedName() {
            return qualifiedName;
        }

        public void setQualifiedName(String qualifiedName) {
            this.qualifiedName = qualifiedName;
        }

        public String getGuid() {
            return guid;
        }

        public void setGuid(String guid) {
            this.guid = guid;
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

        public List<AtlasClassification> getClassifications() {
            return classifications;
        }

        public void setClassifications(List<AtlasClassification> classifications) {
            this.classifications = classifications;
        }

        public Map<String, Object> getCustomAttributes() {
            return customAttributes;
        }

        public void setCustomAttributes(Map<String, Object> customAttributes) {
            this.customAttributes = customAttributes;
        }
    }
}
