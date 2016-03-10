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

package org.apache.atlas.services;

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class ReservedTypesRegistrar implements IBootstrapTypesRegistrar {

    private static final Logger LOG = LoggerFactory.getLogger(ReservedTypesRegistrar.class);

    static String getTypesDir() {
        return System.getProperty("atlas.home")+ File.separator+"models";
    }

    @Override
    public void registerTypes(String typesDirName, TypeSystem typeSystem, MetadataService metadataService)
            throws AtlasException {
        File typesDir = new File(typesDirName);
        if (!typesDir.exists()) {
            LOG.info("No types directory {} found - not registering any reserved types", typesDirName);
            return;
        }
        File[] typeDefFiles = typesDir.listFiles();
        for (File typeDefFile : typeDefFiles) {
            try {
                String typeDefJSON = new String(Files.readAllBytes(typeDefFile.toPath()), StandardCharsets.UTF_8);
                registerType(typeSystem, metadataService, typeDefFile.getAbsolutePath(), typeDefJSON);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    void registerType(TypeSystem typeSystem, MetadataService metadataService, String typeDefName, String typeDefJSON)
            throws AtlasException {
        TypesDef typesDef = null;
        try {
            typesDef = TypesSerialization.fromJson(typeDefJSON);
        } catch (Exception e) {
            LOG.error("Error while deserializing JSON in {}", typeDefName);
            throw new ReservedTypesRegistrationException("Error while deserializing JSON in " + typeDefName, e);
        }
        HierarchicalTypeDefinition<ClassType> classDef = typesDef.classTypesAsJavaList().get(0);
        if (!typeSystem.isRegistered(classDef.typeName)) {
            metadataService.createType(typeDefJSON);
            LOG.info("Registered types in {}", typeDefName);
        } else {
            LOG.warn("class {} already registered, ignoring types in {}", classDef.typeName,
                    typeDefName);
        }
    }
}
