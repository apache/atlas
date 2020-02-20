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

package org.apache.atlas.repository.impexp;

import org.apache.atlas.entitytransform.BaseEntityHandler;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasExportResult;
import org.apache.atlas.model.instance.AtlasEntity;
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
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.apache.atlas.AtlasErrorCode.IMPORT_ATTEMPTING_EMPTY_ZIP;

public class ZipSourceDirect implements EntityImportStream {
    private static final Logger LOG = LoggerFactory.getLogger(ZipSourceDirect.class);

    private final ZipInputStream zipInputStream;
    private int currentPosition;

    private ImportTransforms importTransform;
    private List<BaseEntityHandler> entityHandlers;
    private AtlasTypesDef typesDef;
    private ZipEntry zipEntryNext;
    private int streamSize = 1;

    public ZipSourceDirect(InputStream inputStream, int streamSize) throws IOException, AtlasBaseException {
        this.zipInputStream = new ZipInputStream(inputStream);
        this.streamSize = streamSize;
        prepareStreamForFetch();
    }

    @Override
    public ImportTransforms getImportTransform() { return this.importTransform; }

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
        return this.typesDef;
    }

    @Override
    public
    AtlasExportResult getExportResult() throws AtlasBaseException {
        return new AtlasExportResult();
    }

    @Override
    public List<String> getCreationOrder() {
        return new ArrayList<>();
    }

    @Override
    public int getPosition() {
        return currentPosition;
    }

    @Override
    public AtlasEntity.AtlasEntityWithExtInfo getEntityWithExtInfo(String json) throws AtlasBaseException {
        if (StringUtils.isEmpty(json)) {
            return null;
        }

        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = convertFromJson(AtlasEntity.AtlasEntityWithExtInfo.class, json);

        if (importTransform != null) {
            entityWithExtInfo = importTransform.apply(entityWithExtInfo);
        }

        if (entityHandlers != null) {
            applyTransformers(entityWithExtInfo);
        }

        return entityWithExtInfo;
    }

    @Override
    public boolean hasNext() {
        return (this.zipEntryNext != null
                && !zipEntryNext.getName().equals(ZipExportFileNames.ATLAS_EXPORT_ORDER_NAME.toEntryFileName())
                && !zipEntryNext.getName().equals(ZipExportFileNames.ATLAS_EXPORT_INFO_NAME.toEntryFileName()));
    }

    @Override
    public AtlasEntity next() {
        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = getNextEntityWithExtInfo();

        return entityWithExtInfo != null ? entityWithExtInfo.getEntity() : null;
    }

    @Override
    public AtlasEntity.AtlasEntityWithExtInfo getNextEntityWithExtInfo() {
        try {
            if (hasNext()) {
                String json = moveNext();
                return getEntityWithExtInfo(json);
            }
        } catch (AtlasBaseException e) {
            LOG.error("getNextEntityWithExtInfo", e);
        }
        return null;
    }

    @Override
    public void reset() {
        currentPosition = 0;
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

    @Override
    public void onImportComplete(String guid) {
    }

    @Override
    public void setPosition(int index) {
        try {
            for (int i = 0; i < index; i++) {
                moveNextEntry();
            }
        }
        catch (IOException e) {
            LOG.error("Error setting position: {}. Position may be beyond the stream size.", index);
        }
    }

    @Override
    public void setPositionUsingEntityGuid(String guid) {
    }

    @Override
    public void close() {
    }

    private void applyTransformers(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) {
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
        try {
            return AtlasType.fromJson(jsonData, clazz);

        } catch (Exception e) {
            throw new AtlasBaseException("Error converting file to JSON.", e);
        }
    }

    private AtlasEntity getEntity(String guid) throws AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo extInfo = getEntityWithExtInfo(guid);
        return (extInfo != null) ? extInfo.getEntity() : null;
    }

    public int size() {
        return this.streamSize;
    }

    private String moveNext() {
        try {
            moveNextEntry();
            return getJsonPayloadFromZipEntryStream(this.zipInputStream);
        } catch (IOException e) {
            LOG.error("moveNext failed!", e);
        }

        return null;
    }

    private void moveNextEntry() throws IOException {
        this.zipEntryNext = this.zipInputStream.getNextEntry();
        this.currentPosition++;
    }

    private void prepareStreamForFetch() throws AtlasBaseException, IOException {
        moveNextEntry();
        if (this.zipEntryNext == null) {
            throw new AtlasBaseException(IMPORT_ATTEMPTING_EMPTY_ZIP, "Attempting to import empty ZIP.");
        }

        if (this.zipEntryNext.getName().equals(ZipExportFileNames.ATLAS_TYPESDEF_NAME.toEntryFileName())) {
            String json = getJsonPayloadFromZipEntryStream(this.zipInputStream);
            this.typesDef = AtlasType.fromJson(json, AtlasTypesDef.class);
        }
    }

    private String getJsonPayloadFromZipEntryStream(ZipInputStream zipInputStream) {
        try {
            final int BUFFER_LENGTH = 4096;
            byte[] buf = new byte[BUFFER_LENGTH];

            int n = 0;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            while ((n = zipInputStream.read(buf, 0, BUFFER_LENGTH)) > -1) {
                bos.write(buf, 0, n);
            }

            return bos.toString();
        } catch (IOException ex) {
            LOG.error("Error fetching string from entry!", ex);
        }

        return null;
    }
}
