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
package org.apache.atlas.repository.impexp;

import org.apache.atlas.entitytransform.BaseEntityHandler;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasExportResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.store.graph.v2.EntityImportStream;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.apache.atlas.AtlasErrorCode.IMPORT_ATTEMPTING_EMPTY_ZIP;

public class ZipSource implements EntityImportStream {
    private static final Logger LOG = LoggerFactory.getLogger(ZipSource.class);

    private final InputStream             inputStream;
    private       List<String>            creationOrder;
    private       Iterator<String>        iterator;
    private final Map<String, String>     guidEntityJsonMap;
    private       ImportTransforms        importTransform;
    private       List<BaseEntityHandler> entityHandlers;
    private       int                     currentPosition;
    private       String                  md5Hash;

    public ZipSource(InputStream inputStream) throws IOException, AtlasBaseException {
        this(inputStream, null);
    }

    public ZipSource(InputStream inputStream, ImportTransforms importTransform) throws IOException, AtlasBaseException {
        this.inputStream       = inputStream;
        this.guidEntityJsonMap = new HashMap<>();
        this.importTransform   = importTransform;

        updateGuidZipEntryMap();

        if (isZipFileEmpty()) {
            throw new AtlasBaseException(IMPORT_ATTEMPTING_EMPTY_ZIP, "Attempting to import empty ZIP.");
        }

        setCreationOrder();
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public AtlasEntity next() {
        AtlasEntityWithExtInfo entityWithExtInfo = getNextEntityWithExtInfo();

        return entityWithExtInfo != null ? entityWithExtInfo.getEntity() : null;
    }

    @Override
    public void reset() {
        getCreationOrder();

        this.iterator = this.creationOrder != null ? this.creationOrder.iterator() : Collections.emptyIterator();
    }

    @Override
    public AtlasEntity getByGuid(String guid) {
        try {
            return getEntity(guid);
        } catch (AtlasBaseException e) {
            LOG.error("getByGuid: {} failed!", guid, e);

            return null;
        }
    }

    public int size() {
        return this.creationOrder.size();
    }

    @Override
    public int getPosition() {
        return currentPosition;
    }

    @Override
    public void setPosition(int index) {
        currentPosition = index;

        reset();

        for (int i = 0; i < creationOrder.size() && i <= index; i++) {
            onImportComplete(iterator.next());
        }
    }

    @Override
    public void setPositionUsingEntityGuid(String guid) {
        if (StringUtils.isBlank(guid)) {
            return;
        }

        int index = creationOrder.indexOf(guid);

        if (index == -1) {
            return;
        }

        setPosition(index);
    }

    @Override
    public AtlasEntityWithExtInfo getNextEntityWithExtInfo() {
        try {
            currentPosition++;

            return getEntityWithExtInfo(this.iterator.next());
        } catch (AtlasBaseException e) {
            LOG.warn("getNextEntityWithExtInfo", e);
            return null;
        }
    }

    public AtlasEntityWithExtInfo getEntityWithExtInfo(String guid) throws AtlasBaseException {
        String                 s                 = getFromCache(guid);
        AtlasEntityWithExtInfo entityWithExtInfo = convertFromJson(AtlasEntityWithExtInfo.class, s);

        if (importTransform != null) {
            entityWithExtInfo = importTransform.apply(entityWithExtInfo);
        }

        if (entityHandlers != null) {
            applyTransformers(entityWithExtInfo);
        }

        return entityWithExtInfo;
    }

    @Override
    public void onImportComplete(String guid) {
        guidEntityJsonMap.remove(guid);
    }

    @Override
    public ImportTransforms getImportTransform() {
        return this.importTransform;
    }

    @Override
    public void setImportTransform(ImportTransforms importTransform) {
        this.importTransform = importTransform;
    }

    @Override
    public List<BaseEntityHandler> getEntityHandlers() {
        return entityHandlers;
    }

    @Override
    public void setEntityHandlers(List<BaseEntityHandler> entityHandlers) {
        this.entityHandlers = entityHandlers;
    }

    @Override
    public AtlasTypesDef getTypesDef() throws AtlasBaseException {
        final String fileName = ZipExportFileNames.ATLAS_TYPESDEF_NAME.toString();
        String       s        = getFromCache(fileName);

        return convertFromJson(AtlasTypesDef.class, s);
    }

    @Override
    public AtlasExportResult getExportResult() throws AtlasBaseException {
        final String fileName = ZipExportFileNames.ATLAS_EXPORT_INFO_NAME.toString();
        String       s        = getFromCache(fileName);

        return convertFromJson(AtlasExportResult.class, s);
    }

    @Override
    public List<String> getCreationOrder() {
        return this.creationOrder;
    }

    @Override
    public void close() {
        try {
            inputStream.close();
            guidEntityJsonMap.clear();
        } catch (IOException ex) {
            LOG.warn("Error closing streams.", ex);
        }
    }

    private boolean isZipFileEmpty() {
        if (MapUtils.isEmpty(guidEntityJsonMap)) {
            return true;
        }

        String key = ZipExportFileNames.ATLAS_EXPORT_ORDER_NAME.toString();

        return (guidEntityJsonMap.containsKey(key) && StringUtils.isNotEmpty(guidEntityJsonMap.get(key)) && guidEntityJsonMap.get(key).equals("[]"));
    }

    private void setCreationOrder() {
        String fileName = ZipExportFileNames.ATLAS_EXPORT_ORDER_NAME.toString();

        try {
            String s = getFromCache(fileName);

            this.creationOrder = convertFromJson(List.class, s);

            this.iterator = this.creationOrder != null ? this.creationOrder.iterator() : Collections.emptyIterator();
        } catch (AtlasBaseException e) {
            LOG.error("Error retrieving '{}' from zip.", fileName, e);
        }
    }

    private void updateGuidZipEntryMap() throws IOException {
        try (ZipInputStream zipInputStream = new ZipInputStream(inputStream)) {
            MessageDigest md5Digest = MessageDigest.getInstance("MD5");
            ZipEntry      zipEntry  = zipInputStream.getNextEntry();

            while (zipEntry != null) {
                String entryName = zipEntry.getName().replace(".json", "");

                if (guidEntityJsonMap.containsKey(entryName)) {
                    continue;
                }

                byte[]                buf = new byte[1024];
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                int                   n;

                while ((n = zipInputStream.read(buf, 0, 1024)) > -1) {
                    md5Digest.update(buf, 0, n);
                    bos.write(buf, 0, n);
                }

                guidEntityJsonMap.put(entryName, bos.toString());

                zipEntry = zipInputStream.getNextEntry();
            }

            // Compute the final MD5 hash after processing the entire ZIP file
            byte[]        hashBytes = md5Digest.digest();
            StringBuilder md5Hash   = new StringBuilder();

            for (byte b : hashBytes) {
                md5Hash.append(String.format("%02x", b));
            }

            this.md5Hash = md5Hash.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
    }

    private void applyTransformers(AtlasEntityWithExtInfo entityWithExtInfo) {
        if (entityWithExtInfo == null) {
            return;
        }

        transform(entityWithExtInfo.getEntity());

        if (MapUtils.isNotEmpty(entityWithExtInfo.getReferredEntities())) {
            for (AtlasEntity e : entityWithExtInfo.getReferredEntities().values()) {
                transform(e);
            }
        }
    }

    private void transform(AtlasEntity e) {
        for (BaseEntityHandler handler : entityHandlers) {
            handler.transform(e);
        }
    }

    private <T> T convertFromJson(Class<T> clazz, String jsonData) throws AtlasBaseException {
        T t;

        try {
            t = AtlasType.fromJson(jsonData, clazz);

            if (t == null) {
                LOG.error("Error converting file to JSON.");

                return null;
            }
        } catch (Exception e) {
            throw new AtlasBaseException("Error converting file to JSON.", e);
        }

        return t;
    }

    private String getFromCache(String entryName) {
        String s = guidEntityJsonMap.get(entryName);

        if (StringUtils.isEmpty(s)) {
            LOG.warn("Could not fetch requested contents of file: {}", entryName);
        }

        return s;
    }

    private AtlasEntity getEntity(String guid) throws AtlasBaseException {
        if (guidEntityJsonMap.containsKey(guid)) {
            AtlasEntityWithExtInfo extInfo = getEntityWithExtInfo(guid);

            return (extInfo != null) ? extInfo.getEntity() : null;
        }

        return null;
    }

    @Override
    public String getMd5Hash() {
        return this.md5Hash;
    }
}
