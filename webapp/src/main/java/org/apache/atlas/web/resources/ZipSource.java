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
package org.apache.atlas.web.resources;

import org.codehaus.jackson.type.TypeReference;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasExportResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.store.graph.v1.EntityImportStream;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;


public class ZipSource implements EntityImportStream {
    private static final Logger LOG = LoggerFactory.getLogger(ZipSource.class);

    private final ByteArrayInputStream inputStream;
    private List<String>         creationOrder;
    private Iterator<String>     iterator;

    public ZipSource(ByteArrayInputStream inputStream) {
        this.inputStream = inputStream;

        this.setCreationOrder();
    }

    public AtlasTypesDef getTypesDef() throws AtlasBaseException {
        final String fileName = ZipExportFileNames.ATLAS_TYPESDEF_NAME.toString();

        try {
            String s = get(fileName);
            return convertFromJson(AtlasTypesDef.class, s);
        } catch (IOException e) {
            LOG.error(String.format("Error retrieving '%s' from zip.", fileName), e);
            return null;
        }
    }

    public AtlasExportResult getExportResult() throws AtlasBaseException {
        String fileName = ZipExportFileNames.ATLAS_EXPORT_INFO_NAME.toString();
        try {
            String s = get(fileName);
            return convertFromJson(AtlasExportResult.class, s);
        } catch (IOException e) {
            LOG.error(String.format("Error retrieving '%s' from zip.", fileName), e);
            return null;
        }
    }


    private void setCreationOrder() {
        String fileName = ZipExportFileNames.ATLAS_EXPORT_ORDER_NAME.toString();

        try {
            String s = get(fileName);
            this.creationOrder = convertFromJson(new TypeReference<List<String>>(){}, s);
            this.iterator = this.creationOrder.iterator();
        } catch (IOException e) {
            LOG.error(String.format("Error retrieving '%s' from zip.", fileName), e);
        } catch (AtlasBaseException e) {
            LOG.error(String.format("Error retrieving '%s' from zip.", fileName), e);
        }
    }

    public List<String> getCreationOrder() throws AtlasBaseException {
        return this.creationOrder;
    }

    public AtlasEntity getEntity(String guid) throws AtlasBaseException {
        try {
            String s = get(guid);
            return convertFromJson(AtlasEntity.class, s);
        } catch (IOException e) {
            LOG.error(String.format("Error retrieving '%s' from zip.", guid), e);
            return null;
        }
    }

    private String get(String entryName) throws IOException {
        String ret = "";

        inputStream.reset();

        ZipInputStream zipInputStream = new ZipInputStream(inputStream);
        ZipEntry       zipEntry       = zipInputStream.getNextEntry();

        entryName = entryName + ".json";

        while (zipEntry != null) {
            if (zipEntry.getName().equals(entryName)) {
                break;
            }

            zipEntry = zipInputStream.getNextEntry();
        }

        if (zipEntry != null) {
            ByteArrayOutputStream os  = new ByteArrayOutputStream();
            byte[]                buf = new byte[1024];

            int n = 0;
            while ((n = zipInputStream.read(buf, 0, 1024)) > -1) {
                os.write(buf, 0, n);
            }

            ret = os.toString();
        } else {
            LOG.warn("{}: no such entry in zip file", entryName);
        }

        zipInputStream.close();

        return ret;
    }

    private <T> T convertFromJson(TypeReference clazz, String jsonData) throws AtlasBaseException {
        try {
            ObjectMapper mapper = new ObjectMapper();

            return mapper.readValue(jsonData, clazz);

        } catch (Exception e) {
            throw new AtlasBaseException("Error converting file to JSON.", e);
        }
    }

    private <T> T convertFromJson(Class<T> clazz, String jsonData) throws AtlasBaseException {
        try {
            ObjectMapper mapper = new ObjectMapper();

            return mapper.readValue(jsonData, clazz);

        } catch (Exception e) {
            throw new AtlasBaseException("Error converting file to JSON.", e);
        }
    }

    public void close() throws IOException {
        inputStream.close();
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public AtlasEntity next() {
        try {
            return getEntity(this.iterator.next());
        } catch (AtlasBaseException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void reset() {
        try {
            getCreationOrder();
            this.iterator = this.creationOrder.iterator();
        } catch (AtlasBaseException e) {
            LOG.error("reset", e);
        }
    }

    @Override
    public AtlasEntity getByGuid(String guid)  {
        try {
            return getEntity(guid);
        } catch (AtlasBaseException e) {
            e.printStackTrace();
            return null;
        }
    }
}
