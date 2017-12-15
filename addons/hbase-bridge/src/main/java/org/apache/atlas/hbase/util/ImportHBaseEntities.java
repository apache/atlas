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

package org.apache.atlas.hbase.util;


import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.hook.AtlasHookException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ImportHBaseEntities extends ImportHBaseEntitiesBase {
    private static final Logger LOG = LoggerFactory.getLogger(ImportHBaseEntities.class);

    public static void main(String[] args) throws AtlasHookException {
        try {
            ImportHBaseEntities importHBaseEntities = new ImportHBaseEntities(args);

            importHBaseEntities.execute();
        } catch(Exception e) {
            throw new AtlasHookException("ImportHBaseEntities failed.", e);
        }
    }

    public ImportHBaseEntities(String[] args) throws Exception {
        super(args);
    }

    public boolean execute() throws Exception {
        boolean ret = false;

        if (hbaseAdmin != null) {
            if (StringUtils.isEmpty(namespaceToImport) && StringUtils.isEmpty(tableToImport)) {
                NamespaceDescriptor[] namespaceDescriptors = hbaseAdmin.listNamespaceDescriptors();
                if (!ArrayUtils.isEmpty(namespaceDescriptors)) {
                    for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
                        String namespace = namespaceDescriptor.getName();
                        importNameSpace(namespace);
                    }
                }
                HTableDescriptor[] htds = hbaseAdmin.listTables();
                if (!ArrayUtils.isEmpty(htds)) {
                    for (HTableDescriptor htd : htds) {
                        String tableName = htd.getNameAsString();
                        importTable(tableName);
                    }
                }
                ret = true;
            } else if (StringUtils.isNotEmpty(namespaceToImport)) {
                importNameSpace(namespaceToImport);
                ret = true;
            } else if (StringUtils.isNotEmpty(tableToImport)) {
                importTable(tableToImport);
                ret = true;
            }
        }

        return ret;
    }

    public String importNameSpace(final String nameSpace) throws Exception {
        NamespaceDescriptor namespaceDescriptor = hbaseAdmin.getNamespaceDescriptor(nameSpace);

        createOrUpdateNameSpace(namespaceDescriptor);

        return namespaceDescriptor.getName();
    }

    public String importTable(final String tableName) throws Exception {
        byte[]              tblName      = tableName.getBytes();
        HTableDescriptor    htd          = hbaseAdmin.getTableDescriptor(tblName);
        String              nsName       = htd.getTableName().getNamespaceAsString();
        NamespaceDescriptor nsDescriptor = hbaseAdmin.getNamespaceDescriptor(nsName);
        AtlasEntity         nsEntity     = createOrUpdateNameSpace(nsDescriptor);
        HColumnDescriptor[] hcdts        = htd.getColumnFamilies();
        createOrUpdateTable(nsName, tableName, nsEntity, htd, hcdts);

        return htd.getTableName().getNameAsString();
    }
}
