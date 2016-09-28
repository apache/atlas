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


import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;
import org.apache.atlas.services.AtlasTypePatch.PatchContent;
import org.apache.atlas.services.AtlasTypePatch.PatchData;
import org.apache.atlas.services.AtlasTypePatch.PatchResult;
import org.apache.atlas.services.AtlasTypePatch.PatchStatus;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.TypeUpdateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AtlasPatchHandler {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasPatchHandler.class);

    public static void handlePatches(DefaultMetadataService metadataService, TypeSystem typeSystem) throws TypeUpdateException {

        Map<String, AtlasTypePatch> patchHandlerMap = initializePatchHandlerMap(metadataService, typeSystem);

        if (patchHandlerMap == null || patchHandlerMap.isEmpty())
            return;

        String patchDirName = ReservedTypesRegistrar.getTypesDir() + File.separator + "patches";
        LOG.info("Checking for any patches to be applied to the type system in " + patchDirName);

        File patchDir = new File(patchDirName);
        if (!patchDir.exists()) {
            LOG.info("Patch directory {} doesn't exist, not applying any patches", patchDirName);
            return;
        }

        File[] patchFiles = patchDir.listFiles();
        if (patchFiles == null  || patchFiles.length == 0) {
            LOG.info("No patch files found in {}, not applying any patches", patchDirName);
            return;
        }

        // Sort the patch files based on file name.
        Arrays.sort(patchFiles, new Comparator<File>() {
            public int compare(File f1, File f2) {
                return String.valueOf(f1.getName()).compareTo(f2.getName());
            }
        });

        LOG.info("Found " + patchFiles.length + " patch files to process.");
        int patchNumber = 0;
        Gson gson = initializeGson();
        AtlasTypePatch typePatch;

        for (File patchFile : patchFiles) {
            try {
                LOG.info("Processing patch file " + (++patchNumber) + " - " + patchFile.getAbsolutePath());
                String content = new String(Files.readAllBytes(patchFile.toPath()), StandardCharsets.UTF_8);
                PatchContent patchContent = gson.fromJson(content, PatchContent.class);
                PatchData[] patchDatas = patchContent.getPatches();
                PatchResult result;
                int patchCounter = 0;

                for (PatchData patch : patchDatas) {
                    typePatch = patchHandlerMap.get(patch.getAction());
                    if (typePatch != null) {
                        result = typePatch.applyPatch(patch);

                        if (result != null) {
                            LOG.info(result.getMessage() + " Patch " + (++patchCounter) + " out of " + patchDatas.length + " processed in : " + patchFile.toPath());
                            if (result.getStatus().equals(PatchStatus.FAILED)) {
                                throw new TypeUpdateException(result.getMessage() + " patch " + patchNumber + " failed in :" + patchFile.getAbsolutePath());
                            }
                        }
                    }
                }

            } catch (IOException e) {
                throw new TypeUpdateException("Unable to read patch file from " + patchFile.getAbsolutePath());
            } catch (JsonSyntaxException e) {
                throw new TypeUpdateException("Invalid non-parseable JSON patch file in " + patchFile.getAbsolutePath());
            }
        }

        LOG.info("Processed " + patchFiles.length + " patch files.");
    }

    private static Map<String, AtlasTypePatch> initializePatchHandlerMap(DefaultMetadataService metadataService, TypeSystem typeSystem) {
        Map<String, AtlasTypePatch> patchHandlerMap = new HashMap<String, AtlasTypePatch>();
        List<AtlasTypePatch> patchers = new ArrayList<AtlasTypePatch>();

        // Register new patch classes here
        patchers.add(new AtlasTypeAttributePatch(metadataService, typeSystem));

        for (AtlasTypePatch patcher : patchers) {
            for (String action : patcher.getSupportedActions()) {
                patchHandlerMap.put(action, patcher);
            }
        }

        return patchHandlerMap;
    }

    public static Gson initializeGson() {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(Multiplicity.class, new MultiplicityDeserializer());
        gsonBuilder.setFieldNamingPolicy(FieldNamingPolicy.IDENTITY);
        Gson gson = gsonBuilder.create();

        return gson;
    }

    static class MultiplicityDeserializer implements JsonDeserializer<Multiplicity> {
        @Override
        public Multiplicity deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException {
            String multiplicityString = json.getAsString().toLowerCase();
            Multiplicity m = null;
            switch (multiplicityString) {
                case "optional":
                    m = Multiplicity.OPTIONAL;
                    break;
                case "required":
                    m = Multiplicity.REQUIRED;
                    break;
                case "collection":
                    m = Multiplicity.COLLECTION;
                    break;
                case "set":
                    m = Multiplicity.SET;
                    break;
                default:
                    break;
            }
            return m;
        }
    }
}