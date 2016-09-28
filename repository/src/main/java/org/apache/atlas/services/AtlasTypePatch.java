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

package org.apache.atlas.services;


import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.TypeSystem;

import java.util.Map;


public abstract class AtlasTypePatch {
    protected final TypeSystem typeSystem;
    protected final DefaultMetadataService metadataService;
    protected final String[] supportedActions;

    protected AtlasTypePatch(DefaultMetadataService metadataService, TypeSystem typeSystem, String[] supportedActions) {
        this.metadataService = metadataService;
        this.typeSystem = typeSystem;
        this.supportedActions = supportedActions;
    }

    public final String[] getSupportedActions() { return supportedActions; }

    public abstract PatchResult applyPatch(PatchData patch);

    public enum PatchStatus { SUCCESS, FAILED, SKIPPED }

    public class PatchResult {
        private String message;
        private PatchStatus status;

        public PatchResult(String message, PatchStatus status) {
            this.message = message;
            this.status = status;
        }

        public String getMessage() { return message; }

        public void setMessage(String message) { this.message = message; }

        public PatchStatus getStatus() { return status; }

        public void setStatus(PatchStatus status) { this.status = status; }

    }

    /**
     * A class to capture patch content.
     */
    public class PatchContent {
        private PatchData[] patches;

        public PatchData[] getPatches() {
            return patches;
        }
    }

    public static class PatchData {
        private String action;
        private String typeName;
        private String applyToVersion;
        private String updateToVersion;
        private Map<String, String> params;
        private AttributeDefinition[] attributeDefinitions;

        public PatchData(String action, String typeName, String applyToVersion, String updateToVersion, Map<String, String> params, AttributeDefinition[] attributeDefinitions) {
            this.action = action;
            this.typeName = typeName;
            this.applyToVersion = applyToVersion;
            this.updateToVersion = updateToVersion;
            this.params = params;
            this.attributeDefinitions = attributeDefinitions;
        }

        public String getAction() { return action; }

        public String getTypeName() {  return typeName; }

        public String getApplyToVersion() { return applyToVersion; }

        public String getUpdateToVersion() { return updateToVersion; }

        public Map<String, String> getParams() { return params; }

        public AttributeDefinition[] getAttributeDefinitions() { return attributeDefinitions; }
    }
}
