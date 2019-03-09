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
package org.apache.atlas.repository.store.bootstrap;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAuthorizerFactory;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasEnumDef.AtlasEnumElementDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef.RelationshipCategory;
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Class that handles initial loading of models and patches into typedef store
 */
@Service
public class AtlasTypeDefStoreInitializer implements ActiveStateChangeHandler {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasTypeDefStoreInitializer.class);
    public static final String PATCHES_FOLDER_NAME    = "patches";
    public static final String RELATIONSHIP_LABEL     = "relationshipLabel";
    public static final String RELATIONSHIP_CATEGORY  = "relationshipCategory";
    public static final String RELATIONSHIP_SWAP_ENDS = "swapEnds";

    private final AtlasTypeDefStore atlasTypeDefStore;
    private final AtlasTypeRegistry atlasTypeRegistry;
    private final Configuration     conf;


    @Inject
    public AtlasTypeDefStoreInitializer(AtlasTypeDefStore atlasTypeDefStore, AtlasTypeRegistry atlasTypeRegistry, Configuration conf) {
        this.atlasTypeDefStore = atlasTypeDefStore;
        this.atlasTypeRegistry = atlasTypeRegistry;
        this.conf              = conf;
    }

    @PostConstruct
    public void init() throws AtlasBaseException {
        LOG.info("==> AtlasTypeDefStoreInitializer.init()");

        if (!HAConfiguration.isHAEnabled(conf)) {
            atlasTypeDefStore.init();
            loadBootstrapTypeDefs();

            try {
                AtlasAuthorizerFactory.getAtlasAuthorizer();
            } catch (Throwable t) {
                LOG.error("AtlasTypeDefStoreInitializer.init(): Unable to obtain AtlasAuthorizer", t);
            }
        } else {
            LOG.info("AtlasTypeDefStoreInitializer.init(): deferring type loading until instance activation");
        }

        LOG.info("<== AtlasTypeDefStoreInitializer.init()");
    }

    /**
     * This method is looking for folders in alphabetical order in the models directory. It loads each of these folders and their associated patches in order.
     * It then loads any models in the top level folder and its patches.
     *
     * This allows models to be grouped into folders to help managability.
     *
     */
    private void loadBootstrapTypeDefs() {
        LOG.info("==> AtlasTypeDefStoreInitializer.loadBootstrapTypeDefs()");

        String atlasHomeDir  = System.getProperty("atlas.home");
        String modelsDirName = (StringUtils.isEmpty(atlasHomeDir) ? "." : atlasHomeDir) + File.separator + "models";

        if (modelsDirName == null || modelsDirName.length() == 0) {
            LOG.info("Types directory {} does not exist or not readable or has no typedef files", modelsDirName);
        } else {
            // look for folders we need to load models from
            File   topModeltypesDir  = new File(modelsDirName);
            File[] modelsDirContents = topModeltypesDir.exists() ? topModeltypesDir.listFiles() : null;

            if (modelsDirContents != null && modelsDirContents.length > 0) {
	            Arrays.sort(modelsDirContents);

	            for (File folder : modelsDirContents) {
	                    if (folder.isFile()) {
	                        // ignore files
	                        continue;
	                    } else if (!folder.getName().equals(PATCHES_FOLDER_NAME)){
	                        // load the models alphabetically in the subfolders apart from patches
	                        loadModelsInFolder(folder);
	                    }
	            }
            }

            // load any files in the top models folder and any associated patches.
            loadModelsInFolder(topModeltypesDir);
        }
        LOG.info("<== AtlasTypeDefStoreInitializer.loadBootstrapTypeDefs()");
    }

    /**
     * Load all the model files in the supplied folder followed by the contents of the patches folder.
     * @param typesDir
     */
    private void loadModelsInFolder(File typesDir) {
        LOG.info("==> AtlasTypeDefStoreInitializer({})", typesDir);

        String typesDirName = typesDir.getName();
        File[] typeDefFiles = typesDir.exists() ? typesDir.listFiles() : null;

        if (typeDefFiles == null || typeDefFiles.length == 0) {
            LOG.info("Types directory {} does not exist or not readable or has no typedef files", typesDirName );
        } else {

            // sort the files by filename
            Arrays.sort(typeDefFiles);

            for (File typeDefFile : typeDefFiles) {
                if (typeDefFile.isFile()) {
                    try {
                        String        jsonStr  = new String(Files.readAllBytes(typeDefFile.toPath()), StandardCharsets.UTF_8);
                        AtlasTypesDef typesDef = AtlasType.fromJson(jsonStr, AtlasTypesDef.class);

                        if (typesDef == null || typesDef.isEmpty()) {
                            LOG.info("No type in file {}", typeDefFile.getAbsolutePath());

                            continue;
                        }

                        AtlasTypesDef typesToCreate = getTypesToCreate(typesDef, atlasTypeRegistry);
                        AtlasTypesDef typesToUpdate = getTypesToUpdate(typesDef, atlasTypeRegistry, true);

                        if (!typesToCreate.isEmpty() || !typesToUpdate.isEmpty()) {
                            atlasTypeDefStore.createUpdateTypesDef(typesToCreate, typesToUpdate);

                            LOG.info("Created/Updated types defined in file {}", typeDefFile.getAbsolutePath());
                        } else {
                            LOG.info("No new type in file {}", typeDefFile.getAbsolutePath());
                        }

                    } catch (Throwable t) {
                        LOG.error("error while registering types in file {}", typeDefFile.getAbsolutePath(), t);
                    }
                }
            }

            applyTypePatches(typesDir.getPath());
        }
        LOG.info("<== AtlasTypeDefStoreInitializer({})", typesDir);
    }

    public static AtlasTypesDef getTypesToCreate(AtlasTypesDef typesDef, AtlasTypeRegistry typeRegistry) {
        AtlasTypesDef typesToCreate = new AtlasTypesDef();

        if (CollectionUtils.isNotEmpty(typesDef.getEnumDefs())) {
            for (AtlasEnumDef enumDef : typesDef.getEnumDefs()) {
                if (!typeRegistry.isRegisteredType(enumDef.getName())) {
                    typesToCreate.getEnumDefs().add(enumDef);
                }
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getStructDefs())) {
            for (AtlasStructDef structDef : typesDef.getStructDefs()) {
                if (!typeRegistry.isRegisteredType(structDef.getName())) {
                    typesToCreate.getStructDefs().add(structDef);
                }
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getClassificationDefs())) {
            for (AtlasClassificationDef classificationDef : typesDef.getClassificationDefs()) {
                if (!typeRegistry.isRegisteredType(classificationDef.getName())) {
                    typesToCreate.getClassificationDefs().add(classificationDef);
                }
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getEntityDefs())) {
            for (AtlasEntityDef entityDef : typesDef.getEntityDefs()) {
                if (!typeRegistry.isRegisteredType(entityDef.getName())) {
                    typesToCreate.getEntityDefs().add(entityDef);
                }
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getRelationshipDefs())) {
            for (AtlasRelationshipDef relationshipDef : typesDef.getRelationshipDefs()) {
                if (!typeRegistry.isRegisteredType(relationshipDef.getName())) {
                    typesToCreate.getRelationshipDefs().add(relationshipDef);
                }
            }
        }

        return typesToCreate;
    }

    public static AtlasTypesDef getTypesToUpdate(AtlasTypesDef typesDef, AtlasTypeRegistry typeRegistry, boolean checkTypeVersion) {
        AtlasTypesDef typesToUpdate = new AtlasTypesDef();

        if (CollectionUtils.isNotEmpty(typesDef.getStructDefs())) {
            for (AtlasStructDef newStructDef : typesDef.getStructDefs()) {
                AtlasStructDef  oldStructDef = typeRegistry.getStructDefByName(newStructDef.getName());

                if (oldStructDef == null) {
                    continue;
                }

                if (updateTypeAttributes(oldStructDef, newStructDef, checkTypeVersion)) {
                    typesToUpdate.getStructDefs().add(newStructDef);
                }
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getClassificationDefs())) {
            for (AtlasClassificationDef newClassifDef : typesDef.getClassificationDefs()) {
                AtlasClassificationDef  oldClassifDef = typeRegistry.getClassificationDefByName(newClassifDef.getName());

                if (oldClassifDef == null) {
                    continue;
                }

                if (updateTypeAttributes(oldClassifDef, newClassifDef, checkTypeVersion)) {
                    typesToUpdate.getClassificationDefs().add(newClassifDef);
                }
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getEntityDefs())) {
            for (AtlasEntityDef newEntityDef : typesDef.getEntityDefs()) {
                AtlasEntityDef  oldEntityDef = typeRegistry.getEntityDefByName(newEntityDef.getName());

                if (oldEntityDef == null) {
                    continue;
                }

                if (updateTypeAttributes(oldEntityDef, newEntityDef, checkTypeVersion)) {
                    typesToUpdate.getEntityDefs().add(newEntityDef);
                }
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getEnumDefs())) {
            for (AtlasEnumDef newEnumDef : typesDef.getEnumDefs()) {
                AtlasEnumDef  oldEnumDef = typeRegistry.getEnumDefByName(newEnumDef.getName());

                if (oldEnumDef == null) {
                    continue;
                }

                if (isTypeUpdateApplicable(oldEnumDef, newEnumDef, checkTypeVersion)) {
                    if (CollectionUtils.isNotEmpty(oldEnumDef.getElementDefs())) {
                        for (AtlasEnumElementDef oldEnumElem : oldEnumDef.getElementDefs()) {
                            if (!newEnumDef.hasElement(oldEnumElem.getValue())) {
                                newEnumDef.addElement(oldEnumElem);
                            }
                        }
                    }

                    typesToUpdate.getEnumDefs().add(newEnumDef);
                }
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getRelationshipDefs())) {
            for (AtlasRelationshipDef relationshipDef : typesDef.getRelationshipDefs()) {
                AtlasRelationshipDef  oldRelationshipDef = typeRegistry.getRelationshipDefByName(relationshipDef.getName());

                if (oldRelationshipDef == null) {
                    continue;
                }

                if (updateTypeAttributes(oldRelationshipDef, relationshipDef, checkTypeVersion)) {
                    typesToUpdate.getRelationshipDefs().add(relationshipDef);
                }
            }
        }

        return typesToUpdate;
    }

    @Override
    public void instanceIsActive() throws AtlasException {
        LOG.info("==> AtlasTypeDefStoreInitializer.instanceIsActive()");

        try {
            atlasTypeDefStore.init();

            loadBootstrapTypeDefs();

            try {
                AtlasAuthorizerFactory.getAtlasAuthorizer();
            } catch (Throwable t) {
                LOG.error("AtlasTypeDefStoreInitializer.instanceIsActive(): Unable to obtain AtlasAuthorizer", t);
            }
        } catch (AtlasBaseException e) {
            LOG.error("Failed to init after becoming active", e);
        }

        LOG.info("<== AtlasTypeDefStoreInitializer.instanceIsActive()");
    }

    @Override
    public void instanceIsPassive() throws AtlasException {
        LOG.info("==> AtlasTypeDefStoreInitializer.instanceIsPassive()");

        LOG.info("<== AtlasTypeDefStoreInitializer.instanceIsPassive()");
    }

    @Override
    public int getHandlerOrder() {
        return HandlerOrder.TYPEDEF_STORE_INITIALIZER.getOrder();
    }

    private static boolean updateTypeAttributes(AtlasStructDef oldStructDef, AtlasStructDef newStructDef, boolean checkTypeVersion) {
        boolean ret = isTypeUpdateApplicable(oldStructDef, newStructDef, checkTypeVersion);

        if (ret) {
            // make sure that all attributes in oldDef are in newDef as well
            if (CollectionUtils.isNotEmpty(oldStructDef.getAttributeDefs())){
                for (AtlasAttributeDef oldAttrDef : oldStructDef.getAttributeDefs()) {
                    if (!newStructDef.hasAttribute(oldAttrDef.getName())) {
                        newStructDef.addAttribute(oldAttrDef);
                    }
                }
            }
        }

        return ret;
    }

    private static boolean isTypeUpdateApplicable(AtlasBaseTypeDef oldTypeDef, AtlasBaseTypeDef newTypeDef, boolean checkVersion) {
        boolean ret = true;

        if (checkVersion) {
            String oldTypeVersion = oldTypeDef.getTypeVersion();
            String newTypeVersion = newTypeDef.getTypeVersion();

            ret = ObjectUtils.compare(newTypeVersion, oldTypeVersion) > 0;
        }

        return ret;
    }

    private void applyTypePatches(String typesDirName) {
        String typePatchesDirName = typesDirName + File.separator + PATCHES_FOLDER_NAME;
        File   typePatchesDir     = new File(typePatchesDirName);
        File[] typePatchFiles     = typePatchesDir.exists() ? typePatchesDir.listFiles() : null;

        if (typePatchFiles == null || typePatchFiles.length == 0) {
            LOG.info("Type patches directory {} does not exist or not readable or has no patches", typePatchesDirName);
        } else {
            LOG.info("Type patches directory {} is being processed", typePatchesDirName);

            // sort the files by filename
            Arrays.sort(typePatchFiles);

            PatchHandler[] patchHandlers = new PatchHandler[] {
                    new AddAttributePatchHandler(atlasTypeDefStore, atlasTypeRegistry),
                    new UpdateAttributePatchHandler(atlasTypeDefStore, atlasTypeRegistry),
                    new RemoveLegacyRefAttributesPatchHandler(atlasTypeDefStore, atlasTypeRegistry),
                    new UpdateTypeDefOptionsPatchHandler(atlasTypeDefStore, atlasTypeRegistry),
                    new SetServiceTypePatchHandler(atlasTypeDefStore, atlasTypeRegistry)
            };

            Map<String, PatchHandler> patchHandlerRegistry = new HashMap<>();

            for (PatchHandler patchHandler : patchHandlers) {
                for (String supportedAction : patchHandler.getSupportedActions()) {
                    patchHandlerRegistry.put(supportedAction, patchHandler);
                }
            }

            for (File typePatchFile : typePatchFiles) {
                if (typePatchFile.isFile()) {

                    LOG.info("Applying patches in file {}", typePatchFile.getAbsolutePath());

                    try {
                        String jsonStr = new String(Files.readAllBytes(typePatchFile.toPath()), StandardCharsets.UTF_8);
                        TypeDefPatches patches = AtlasType.fromJson(jsonStr, TypeDefPatches.class);

                        if (patches == null || CollectionUtils.isEmpty(patches.getPatches())) {
                            LOG.info("No patches in file {}", typePatchFile.getAbsolutePath());

                            continue;
                        }

                        for (TypeDefPatch patch : patches.getPatches()) {
                            PatchHandler patchHandler = patchHandlerRegistry.get(patch.getAction());

                            if (patchHandler == null) {
                                LOG.error("Unknown patch action {} in file {}. Ignored",
                                        patch.getAction(), typePatchFile.getAbsolutePath());

                                continue;
                            }

                            try {
                                patchHandler.applyPatch(patch);
                            } catch (AtlasBaseException excp) {
                                LOG.error("Failed to apply {} patch in file {}. Ignored", patch.getAction(), typePatchFile.getAbsolutePath(), excp);
                            }
                        }
                    } catch (Throwable t) {
                        LOG.error("Failed to apply patches in file {}. Ignored", typePatchFile.getAbsolutePath(), t);
                    }
                }
            }
        }
    }

    /**
     * typedef patch details
     */
    @JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    static class TypeDefPatch {
        private String                  action;
        private String                  typeName;
        private String                  applyToVersion;
        private String                  updateToVersion;
        private Map<String, Object>     params;
        private List<AtlasAttributeDef> attributeDefs;
        private Map<String, String>     typeDefOptions;
        private String                  serviceType;

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }

        public String getTypeName() {
            return typeName;
        }

        public void setTypeName(String typeName) {
            this.typeName = typeName;
        }

        public String getApplyToVersion() {
            return applyToVersion;
        }

        public void setApplyToVersion(String applyToVersion) {
            this.applyToVersion = applyToVersion;
        }

        public String getUpdateToVersion() {
            return updateToVersion;
        }

        public void setUpdateToVersion(String updateToVersion) {
            this.updateToVersion = updateToVersion;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public void setParams(Map<String, Object> params) {
            this.params = params;
        }

        public List<AtlasAttributeDef> getAttributeDefs() {
            return attributeDefs;
        }

        public void setAttributeDefs(List<AtlasAttributeDef> attributeDefs) {
            this.attributeDefs = attributeDefs;
        }

        public Map<String, String> getTypeDefOptions() {
            return typeDefOptions;
        }

        public void setTypeDefOptions(Map<String, String> typeDefOptions) {
            this.typeDefOptions = typeDefOptions;
        }

        public String getServiceType() {
            return serviceType;
        }

        public void setServiceType(String serviceType) {
            this.serviceType = serviceType;
        }
    }

    /**
     * list of typedef patches
     */
    @JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    static class TypeDefPatches {
        private List<TypeDefPatch> patches;

        public List<TypeDefPatch> getPatches() {
            return patches;
        }

        public void setPatches(List<TypeDefPatch> patches) {
            this.patches = patches;
        }
    }

    abstract class PatchHandler {
        protected final AtlasTypeDefStore typeDefStore;
        protected final AtlasTypeRegistry typeRegistry;
        protected final String[]          supportedActions;

        protected PatchHandler(AtlasTypeDefStore typeDefStore, AtlasTypeRegistry typeRegistry, String[] supportedActions) {
            this.typeDefStore     = typeDefStore;
            this.typeRegistry     = typeRegistry;
            this.supportedActions = supportedActions;
        }

        public String[] getSupportedActions() { return supportedActions; }

        public abstract void applyPatch(TypeDefPatch patch) throws AtlasBaseException;

        protected boolean isPatchApplicable(TypeDefPatch patch, AtlasBaseTypeDef currentTypeDef) {
            String currentVersion = currentTypeDef.getTypeVersion();
            String applyToVersion = patch.getApplyToVersion();

            return currentVersion == null ||
                   currentVersion.equalsIgnoreCase(applyToVersion) ||
                   currentVersion.startsWith(applyToVersion + ".");
        }
    }

    class AddAttributePatchHandler extends PatchHandler {
        public AddAttributePatchHandler(AtlasTypeDefStore typeDefStore, AtlasTypeRegistry typeRegistry) {
            super(typeDefStore, typeRegistry, new String[] { "ADD_ATTRIBUTE" });
        }

        @Override
        public void applyPatch(TypeDefPatch patch) throws AtlasBaseException {
            String           typeName = patch.getTypeName();
            AtlasBaseTypeDef typeDef  = typeRegistry.getTypeDefByName(typeName);

            if (typeDef == null) {
                throw new AtlasBaseException(AtlasErrorCode.PATCH_FOR_UNKNOWN_TYPE, patch.getAction(), typeName);
            }

            if (isPatchApplicable(patch, typeDef)) {
                if (typeDef.getClass().equals(AtlasEntityDef.class)) {
                    AtlasEntityDef updatedDef = new AtlasEntityDef((AtlasEntityDef)typeDef);

                    for (AtlasAttributeDef attributeDef : patch.getAttributeDefs()) {
                        updatedDef.addAttribute(attributeDef);
                    }
                    updatedDef.setTypeVersion(patch.getUpdateToVersion());

                    typeDefStore.updateEntityDefByName(typeName, updatedDef);
                } else if (typeDef.getClass().equals(AtlasClassificationDef.class)) {
                    AtlasClassificationDef updatedDef = new AtlasClassificationDef((AtlasClassificationDef)typeDef);

                    for (AtlasAttributeDef attributeDef : patch.getAttributeDefs()) {
                        updatedDef.addAttribute(attributeDef);
                    }
                    updatedDef.setTypeVersion(patch.getUpdateToVersion());

                    typeDefStore.updateClassificationDefByName(typeName, updatedDef);
                } else if (typeDef.getClass().equals(AtlasStructDef.class)) {
                    AtlasStructDef updatedDef = new AtlasStructDef((AtlasStructDef)typeDef);

                    for (AtlasAttributeDef attributeDef : patch.getAttributeDefs()) {
                        updatedDef.addAttribute(attributeDef);
                    }
                    updatedDef.setTypeVersion(patch.getUpdateToVersion());

                    typeDefStore.updateStructDefByName(typeName, updatedDef);
                } else {
                    throw new AtlasBaseException(AtlasErrorCode.PATCH_NOT_APPLICABLE_FOR_TYPE,
                            patch.getAction(), typeDef.getClass().getSimpleName());
                }
            } else {
                LOG.info("patch skipped: typeName={}; applyToVersion={}; updateToVersion={}",
                        patch.getTypeName(), patch.getApplyToVersion(), patch.getUpdateToVersion());
            }
        }
    }

    class UpdateAttributePatchHandler extends PatchHandler {
        public UpdateAttributePatchHandler(AtlasTypeDefStore typeDefStore, AtlasTypeRegistry typeRegistry) {
            super(typeDefStore, typeRegistry, new String[] { "UPDATE_ATTRIBUTE" });
        }

        @Override
        public void applyPatch(TypeDefPatch patch) throws AtlasBaseException {
            String           typeName = patch.getTypeName();
            AtlasBaseTypeDef typeDef  = typeRegistry.getTypeDefByName(typeName);

            if (typeDef == null) {
                throw new AtlasBaseException(AtlasErrorCode.PATCH_FOR_UNKNOWN_TYPE, patch.getAction(), typeName);
            }

            if (isPatchApplicable(patch, typeDef)) {
                if (typeDef.getClass().equals(AtlasEntityDef.class)) {
                    AtlasEntityDef updatedDef = new AtlasEntityDef((AtlasEntityDef)typeDef);

                    addOrUpdateAttributes(updatedDef, patch.getAttributeDefs());

                    updatedDef.setTypeVersion(patch.getUpdateToVersion());

                    typeDefStore.updateEntityDefByName(typeName, updatedDef);
                } else if (typeDef.getClass().equals(AtlasClassificationDef.class)) {
                    AtlasClassificationDef updatedDef = new AtlasClassificationDef((AtlasClassificationDef)typeDef);

                    addOrUpdateAttributes(updatedDef, patch.getAttributeDefs());

                    updatedDef.setTypeVersion(patch.getUpdateToVersion());

                    typeDefStore.updateClassificationDefByName(typeName, updatedDef);
                } else if (typeDef.getClass().equals(AtlasStructDef.class)) {
                    AtlasStructDef updatedDef = new AtlasStructDef((AtlasStructDef)typeDef);

                    addOrUpdateAttributes(updatedDef, patch.getAttributeDefs());

                    updatedDef.setTypeVersion(patch.getUpdateToVersion());

                    typeDefStore.updateStructDefByName(typeName, updatedDef);

                } else {
                    throw new AtlasBaseException(AtlasErrorCode.PATCH_NOT_APPLICABLE_FOR_TYPE,
                                                 patch.getAction(), typeDef.getClass().getSimpleName());
                }
            } else {
                LOG.info("patch skipped: typeName={}; applyToVersion={}; updateToVersion={}",
                          patch.getTypeName(), patch.getApplyToVersion(), patch.getUpdateToVersion());
            }
        }

        private void addOrUpdateAttributes(AtlasStructDef structDef, List<AtlasAttributeDef> attributesToUpdate) {
            for (AtlasAttributeDef attributeToUpdate : attributesToUpdate) {
                String attrName = attributeToUpdate.getName();

                if (structDef.hasAttribute(attrName)) {
                    structDef.removeAttribute(attrName);
                }

                structDef.addAttribute(attributeToUpdate);
            }
        }
    }

    class RemoveLegacyRefAttributesPatchHandler extends PatchHandler {
        public RemoveLegacyRefAttributesPatchHandler(AtlasTypeDefStore typeDefStore, AtlasTypeRegistry typeRegistry) {
            super(typeDefStore, typeRegistry, new String[] { "REMOVE_LEGACY_REF_ATTRIBUTES" });
        }

        @Override
        public void applyPatch(TypeDefPatch patch) throws AtlasBaseException {
            String           typeName = patch.getTypeName();
            AtlasBaseTypeDef typeDef  = typeRegistry.getTypeDefByName(typeName);

            if (typeDef == null) {
                throw new AtlasBaseException(AtlasErrorCode.PATCH_FOR_UNKNOWN_TYPE, patch.getAction(), typeName);
            }

            if (isPatchApplicable(patch, typeDef)) {
                if (typeDef.getClass().equals(AtlasRelationshipDef.class)) {
                    AtlasRelationshipDef    relationshipDef = (AtlasRelationshipDef) typeDef;
                    AtlasRelationshipEndDef end1Def         = relationshipDef.getEndDef1();
                    AtlasRelationshipEndDef end2Def         = relationshipDef.getEndDef2();
                    AtlasEntityType         end1Type        = typeRegistry.getEntityTypeByName(end1Def.getType());
                    AtlasEntityType         end2Type        = typeRegistry.getEntityTypeByName(end2Def.getType());

                    String               newRelationshipLabel    = null;
                    RelationshipCategory newRelationshipCategory = null;
                    boolean              swapEnds                = false;

                    if (patch.getParams() != null) {
                        Object relLabel    = patch.getParams().get(RELATIONSHIP_LABEL);
                        Object relCategory = patch.getParams().get(RELATIONSHIP_CATEGORY);
                        Object relSwapEnds = patch.getParams().get(RELATIONSHIP_SWAP_ENDS);

                        if (relLabel != null) {
                            newRelationshipLabel = relLabel.toString();
                        }

                        if (relCategory != null) {
                            newRelationshipCategory = RelationshipCategory.valueOf(relCategory.toString());
                        }

                        if (relSwapEnds != null) {
                            swapEnds = Boolean.valueOf(relSwapEnds.toString());
                        }
                    }

                    if (StringUtils.isEmpty(newRelationshipLabel)) {
                        if (end1Def.getIsLegacyAttribute()) {
                            if (!end2Def.getIsLegacyAttribute()) {
                                AtlasAttribute legacyAttribute = end1Type.getAttribute(end1Def.getName());

                                newRelationshipLabel = "__" + legacyAttribute.getQualifiedName();
                            } else { // if both ends are legacy attributes, RELATIONSHIP_LABEL should be specified in the patch
                                throw new AtlasBaseException(AtlasErrorCode.PATCH_MISSING_RELATIONSHIP_LABEL, patch.getAction(), typeName);
                            }
                        } else if (end2Def.getIsLegacyAttribute()) {
                            AtlasAttribute legacyAttribute = end2Type.getAttribute(end2Def.getName());

                            newRelationshipLabel = "__" + legacyAttribute.getQualifiedName();
                        } else {
                            newRelationshipLabel = relationshipDef.getRelationshipLabel();
                        }
                    }

                    AtlasRelationshipDef updatedDef = new AtlasRelationshipDef(relationshipDef);

                    if (swapEnds) {
                        AtlasRelationshipEndDef tmp = updatedDef.getEndDef1();

                        updatedDef.setEndDef1(updatedDef.getEndDef2());
                        updatedDef.setEndDef2(tmp);
                    }

                    end1Def  = updatedDef.getEndDef1();
                    end2Def  = updatedDef.getEndDef2();
                    end1Type = typeRegistry.getEntityTypeByName(end1Def.getType());
                    end2Type = typeRegistry.getEntityTypeByName(end2Def.getType());

                    updatedDef.setRelationshipLabel(newRelationshipLabel);

                    if (newRelationshipCategory != null) {
                        updatedDef.setRelationshipCategory(newRelationshipCategory);
                    }

                    updatedDef.setTypeVersion(patch.getUpdateToVersion());

                    AtlasEntityDef updatedEntityDef1 = new AtlasEntityDef(end1Type.getEntityDef());
                    AtlasEntityDef updatedEntityDef2 = new AtlasEntityDef(end2Type.getEntityDef());

                    updatedEntityDef1.removeAttribute(end1Def.getName());
                    updatedEntityDef2.removeAttribute(end2Def.getName());

                    AtlasTypesDef typesDef = new AtlasTypesDef();

                    typesDef.setEntityDefs(Arrays.asList(updatedEntityDef1, updatedEntityDef2));
                    typesDef.setRelationshipDefs(Collections.singletonList(updatedDef));

                    try {
                        RequestContext.get().setInTypePatching(true); // to allow removal of attributes

                        typeDefStore.updateTypesDef(typesDef);
                    } finally {
                        RequestContext.get().setInTypePatching(false);
                    }
                }
            } else {
                LOG.info("patch skipped: typeName={}; applyToVersion={}; updateToVersion={}",
                         patch.getTypeName(), patch.getApplyToVersion(), patch.getUpdateToVersion());
            }
        }
    }

    class UpdateTypeDefOptionsPatchHandler extends PatchHandler {
        public UpdateTypeDefOptionsPatchHandler(AtlasTypeDefStore typeDefStore, AtlasTypeRegistry typeRegistry) {
            super(typeDefStore, typeRegistry, new String[] { "UPDATE_TYPEDEF_OPTIONS" });
        }

        @Override
        public void applyPatch(TypeDefPatch patch) throws AtlasBaseException {
            String           typeName = patch.getTypeName();
            AtlasBaseTypeDef typeDef  = typeRegistry.getTypeDefByName(typeName);

            if (typeDef == null) {
                throw new AtlasBaseException(AtlasErrorCode.PATCH_FOR_UNKNOWN_TYPE, patch.getAction(), typeName);
            }

            if (MapUtils.isEmpty(patch.getTypeDefOptions())) {
                throw new AtlasBaseException(AtlasErrorCode.PATCH_INVALID_DATA, patch.getAction(), typeName);
            }

            if (isPatchApplicable(patch, typeDef)) {
                if (typeDef.getOptions() == null) {
                    typeDef.setOptions(patch.getTypeDefOptions());
                } else {
                    typeDef.getOptions().putAll(patch.getTypeDefOptions());
                }
                typeDef.setTypeVersion(patch.getUpdateToVersion());

                typeDefStore.updateTypesDef(AtlasTypeUtil.getTypesDef(typeDef));
            } else {
                LOG.info("patch skipped: typeName={}; applyToVersion={}; updateToVersion={}",
                         patch.getTypeName(), patch.getApplyToVersion(), patch.getUpdateToVersion());
            }
        }
    }

    class SetServiceTypePatchHandler extends PatchHandler {
        public SetServiceTypePatchHandler(AtlasTypeDefStore typeDefStore, AtlasTypeRegistry typeRegistry) {
            super(typeDefStore, typeRegistry, new String[] { "SET_SERVICE_TYPE" });
        }

        @Override
        public void applyPatch(TypeDefPatch patch) throws AtlasBaseException {
            String           typeName = patch.getTypeName();
            AtlasBaseTypeDef typeDef  = typeRegistry.getTypeDefByName(typeName);

            if (typeDef == null) {
                throw new AtlasBaseException(AtlasErrorCode.PATCH_FOR_UNKNOWN_TYPE, patch.getAction(), typeName);
            }

            if (isPatchApplicable(patch, typeDef)) {
                typeDef.setServiceType(patch.getServiceType());
                typeDef.setTypeVersion(patch.getUpdateToVersion());

                typeDefStore.updateTypesDef(AtlasTypeUtil.getTypesDef(typeDef));
            } else {
                LOG.info("patch skipped: typeName={}; applyToVersion={}; updateToVersion={}",
                        patch.getTypeName(), patch.getApplyToVersion(), patch.getUpdateToVersion());
            }
        }
    }
}
