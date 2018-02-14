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

package org.apache.atlas.hbase.hook;


import org.apache.atlas.hbase.bridge.HBaseAtlasHook;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HBaseAtlasCoprocessor extends HBaseAtlasCoprocessorBase {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseAtlasCoprocessor.class);

    final HBaseAtlasHook hbaseAtlasHook;

    public HBaseAtlasCoprocessor() {
        hbaseAtlasHook = HBaseAtlasHook.getInstance();
    }

    @Override
    public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> observerContext, HTableDescriptor hTableDescriptor, HRegionInfo[] hRegionInfos) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessoror.postCreateTable()");
        }
        hbaseAtlasHook.sendHBaseTableOperation(hTableDescriptor, null, HBaseAtlasHook.OPERATION.CREATE_TABLE);
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessoror.postCreateTable()");
        }
    }

    @Override
    public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postDeleteTable()");
        }
        hbaseAtlasHook.sendHBaseTableOperation(null, tableName, HBaseAtlasHook.OPERATION.DELETE_TABLE);
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postDeleteTable()");
        }
    }

    @Override
    public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName, HTableDescriptor hTableDescriptor) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postModifyTable()");
        }
        hbaseAtlasHook.sendHBaseTableOperation(hTableDescriptor, tableName, HBaseAtlasHook.OPERATION.ALTER_TABLE);
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postModifyTable()");
        }
    }

    @Override
    public void postAddColumn(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName, HColumnDescriptor hColumnDescriptor) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postAddColumn()");
        }
        hbaseAtlasHook.sendHBaseColumnFamilyOperation(hColumnDescriptor, tableName, null, HBaseAtlasHook.OPERATION.CREATE_COLUMN_FAMILY);
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postAddColumn()");
        }
    }

    @Override
    public void postModifyColumn(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName, HColumnDescriptor hColumnDescriptor) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postModifyColumn()");
        }
        hbaseAtlasHook.sendHBaseColumnFamilyOperation(hColumnDescriptor, tableName, null, HBaseAtlasHook.OPERATION.ALTER_COLUMN_FAMILY);
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postModifyColumn()");
        }
    }

    @Override
    public void postDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName, byte[] bytes) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postDeleteColumn()");
        }

        String columnFamily = Bytes.toString(bytes);
        hbaseAtlasHook.sendHBaseColumnFamilyOperation(null, tableName, columnFamily, HBaseAtlasHook.OPERATION.DELETE_COLUMN_FAMILY);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postDeleteColumn()");
        }
    }

    @Override
    public void postCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> observerContext, NamespaceDescriptor namespaceDescriptor) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postCreateNamespace()");
        }

        hbaseAtlasHook.sendHBaseNameSpaceOperation(namespaceDescriptor, null, HBaseAtlasHook.OPERATION.CREATE_NAMESPACE);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postCreateNamespace()");
        }
    }

    @Override
    public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> observerContext, String s) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postDeleteNamespace()");
        }

        hbaseAtlasHook.sendHBaseNameSpaceOperation(null, s, HBaseAtlasHook.OPERATION.DELETE_NAMESPACE);

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postDeleteNamespace()");
        }
    }

    @Override
    public void postModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> observerContext, NamespaceDescriptor namespaceDescriptor) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postModifyNamespace()");
        }

        hbaseAtlasHook.sendHBaseNameSpaceOperation(namespaceDescriptor, null, HBaseAtlasHook.OPERATION.ALTER_NAMESPACE);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postModifyNamespace()");
        }
    }

    @Override
    public void postCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> observerContext, HBaseProtos.SnapshotDescription snapshotDescription, HTableDescriptor hTableDescriptor) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessoror.postCloneSnapshot()");
        }
        hbaseAtlasHook.sendHBaseTableOperation(hTableDescriptor, null, HBaseAtlasHook.OPERATION.CREATE_TABLE);
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessoror.postCloneSnapshot()");
        }

    }

    @Override
    public void postRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> observerContext, HBaseProtos.SnapshotDescription snapshotDescription, HTableDescriptor hTableDescriptor) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HBaseAtlasCoprocessor.postRestoreSnapshot()");
        }
        hbaseAtlasHook.sendHBaseTableOperation(hTableDescriptor, hTableDescriptor.getTableName(), HBaseAtlasHook.OPERATION.ALTER_TABLE);
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HBaseAtlasCoprocessor.postRestoreSnapshot()");
        }
    }

}


