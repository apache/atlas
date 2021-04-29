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

import java.util.Objects;

@AtlasJSON
public class AtlasGlossaryTermHeader {
    private String glossaryGuid;
    private String qualifiedName;

    public AtlasGlossaryTermHeader(String glossaryGuid) {
        this.glossaryGuid = glossaryGuid;
    }

    public AtlasGlossaryTermHeader(String glossaryGuid, String qualifiedName) {
        this.glossaryGuid = glossaryGuid;
        this.qualifiedName = qualifiedName;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AtlasGlossaryTermHeader{");
        sb.append("glossaryGuid='").append(glossaryGuid).append('\'');
        sb.append(", qualifiedName='").append(qualifiedName).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public AtlasGlossaryTermHeader() {
    }

    public String getGlossaryGuid() {
        return glossaryGuid;
    }

    public void setGlossaryGuid(final String glossaryGuid) {
        this.glossaryGuid = glossaryGuid;
    }

    public String getQualifiedName() {
        return qualifiedName;
    }

    public void setQualifiedName(final String qualifiedName) {
        this.qualifiedName = qualifiedName;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof org.apache.atlas.model.glossary.AtlasGlossaryTermHeader)) return false;
        final org.apache.atlas.model.glossary.AtlasGlossaryTermHeader that = (org.apache.atlas.model.glossary.AtlasGlossaryTermHeader) o;
        return Objects.equals(glossaryGuid, that.glossaryGuid) &&
                Objects.equals(qualifiedName, that.qualifiedName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(glossaryGuid, qualifiedName);
    }

}
