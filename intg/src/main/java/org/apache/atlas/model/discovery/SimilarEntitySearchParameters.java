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
package org.apache.atlas.model.discovery;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SimilarEntitySearchParameters implements Serializable {
    private static final long serialVersionUID = 1L;

    private String      typeName;
    private boolean     excludeDeletedEntities = true;
    private boolean     includeSubTypes        = true;
    private int         topK;
    private double      minScore;
    private Set<String> attributes;

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public boolean getExcludeDeletedEntities() {
        return excludeDeletedEntities;
    }

    public void setExcludeDeletedEntities(boolean excludeDeletedEntities) {
        this.excludeDeletedEntities = excludeDeletedEntities;
    }

    public boolean getIncludeSubTypes() {
        return includeSubTypes;
    }

    public void setIncludeSubTypes(boolean includeSubTypes) {
        this.includeSubTypes = includeSubTypes;
    }

    public int getTopK() {
        return topK;
    }

    public void setTopK(int topK) {
        this.topK = topK;
    }

    public double getMinScore() {
        return minScore;
    }

    public void setMinScore(double minScore) {
        this.minScore = minScore;
    }

    public Set<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Set<String> attributes) {
        this.attributes = attributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimilarEntitySearchParameters that = (SimilarEntitySearchParameters) o;
        return excludeDeletedEntities == that.excludeDeletedEntities &&
                includeSubTypes == that.includeSubTypes &&
                topK == that.topK &&
                Double.compare(that.minScore, minScore) == 0 &&
                Objects.equals(typeName, that.typeName) &&
                Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeName, excludeDeletedEntities, includeSubTypes, topK, minScore, attributes);
    }
}
