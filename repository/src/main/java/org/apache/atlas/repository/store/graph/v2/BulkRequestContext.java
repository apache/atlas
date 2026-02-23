/**
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
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.type.AtlasType;

import java.util.HashMap;
import java.util.Map;

public class BulkRequestContext {
    private boolean replaceClassifications;
    private boolean replaceTags;
    private boolean appendTags;

    private boolean replaceBusinessAttributes;
    private boolean isOverwriteBusinessAttributes;

    private AtlasEntitiesWithExtInfo originalEntities;  // for async ingestion Kafka publish
    private boolean skipProcessEdgeRestoration;         // query param from REST

    public boolean isReplaceClassifications() {
        return replaceClassifications;
    }

    public boolean isReplaceTags() {
        return replaceTags;
    }

    public boolean isAppendTags() {
        return appendTags;
    }

    public boolean isReplaceBusinessAttributes() {
        return replaceBusinessAttributes;
    }

    public boolean isOverwriteBusinessAttributes() {
        return isOverwriteBusinessAttributes;
    }

    public AtlasEntitiesWithExtInfo getOriginalEntities() {
        return originalEntities;
    }

    public boolean isSkipProcessEdgeRestoration() {
        return skipProcessEdgeRestoration;
    }

    /**
     * Build an operation metadata map for async ingestion Kafka events.
     */
    public Map<String, Object> toOperationMetadata() {
        Map<String, Object> meta = new HashMap<>();
        meta.put("replaceClassifications", replaceClassifications);
        meta.put("replaceTags", replaceTags);
        meta.put("appendTags", appendTags);
        meta.put("replaceBusinessAttributes", replaceBusinessAttributes);
        meta.put("overwriteBusinessAttributes", isOverwriteBusinessAttributes);
        meta.put("skipProcessEdgeRestoration", skipProcessEdgeRestoration);
        return meta;
    }

    public BulkRequestContext() {
        this.replaceClassifications = false;
        this.replaceTags = false;
        this.appendTags = false;

        this.replaceBusinessAttributes = false;
        this.isOverwriteBusinessAttributes = false;
        this.skipProcessEdgeRestoration = false;
    }

    private BulkRequestContext(Builder builder) {
        this.replaceClassifications = builder.replaceClassifications;
        this.replaceTags = builder.replaceTags;
        this.appendTags = builder.appendTags;

        this.replaceBusinessAttributes = builder.replaceBusinessAttributes;
        this.isOverwriteBusinessAttributes = builder.isOverwriteBusinessAttributes;
        this.originalEntities = builder.originalEntities;
        this.skipProcessEdgeRestoration = builder.skipProcessEdgeRestoration;
    }

    public static class Builder {
        private boolean replaceClassifications = false;
        private boolean replaceTags = false;
        private boolean appendTags = false;

        private boolean replaceBusinessAttributes = false;
        private boolean isOverwriteBusinessAttributes = false;
        private AtlasEntitiesWithExtInfo originalEntities;
        private boolean skipProcessEdgeRestoration = false;

        public Builder setReplaceClassifications(boolean replaceClassifications) {
            this.replaceClassifications = replaceClassifications;

            if (replaceClassifications) {
                this.replaceTags = false;
                this.appendTags = false;
            }
            return this;
        }

        public Builder setReplaceTags(boolean replaceTags) {
            this.replaceTags = replaceTags;

            if (replaceTags) {
                this.replaceClassifications = false;
                this.appendTags = false;
            }
            return this;
        }

        public Builder setAppendTags(boolean appendTags) {
            this.appendTags = appendTags;

            if (appendTags) {
                this.replaceTags = false;
                this.replaceClassifications = false;
            }
            return this;
        }

        public Builder setReplaceBusinessAttributes(boolean replaceBusinessAttributes) {
            this.replaceBusinessAttributes = replaceBusinessAttributes;
            return this;
        }

        public Builder setOverwriteBusinessAttributes(boolean overwriteBusinessAttributes) {
            isOverwriteBusinessAttributes = overwriteBusinessAttributes;
            return this;
        }

        public Builder setOriginalEntities(AtlasEntitiesWithExtInfo originalEntities) {
            this.originalEntities = AtlasType.fromJson(AtlasType.toJson(originalEntities), AtlasEntitiesWithExtInfo.class);
            return this;
        }

        public Builder setSkipProcessEdgeRestoration(boolean skipProcessEdgeRestoration) {
            this.skipProcessEdgeRestoration = skipProcessEdgeRestoration;
            return this;
        }

        public BulkRequestContext build() {
            return new BulkRequestContext(this);
        }
    }
}
