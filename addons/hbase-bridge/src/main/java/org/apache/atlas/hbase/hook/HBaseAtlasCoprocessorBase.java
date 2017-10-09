/**
 *
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
package org.apache.atlas.hbase.hook;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;


import com.google.common.collect.ImmutableList;
import org.apache.atlas.hook.AtlasHook;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.*;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.protobuf.generated.*;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALKey;


/**
 * This class exists only to prevent the clutter of methods that we don't intend to implement in the main co-processor class.
 *
 */
public abstract class HBaseAtlasCoprocessorBase implements MasterObserver,RegionObserver,RegionServerObserver,BulkLoadObserver {

    @Override
    public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> observerContext, HTableDescriptor hTableDescriptor, HRegionInfo[] hRegionInfos) throws IOException {

    }

    @Override
    public void preCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> observerContext, HTableDescriptor hTableDescriptor, HRegionInfo[] hRegionInfos) throws IOException {

    }

    @Override
    public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName) throws IOException {

    }

    @Override
    public void preDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName) throws IOException {

    }

    @Override
    public void preTruncateTable(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName) throws IOException {

    }

    @Override
    public void preTruncateTableHandler(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName) throws IOException {

    }

    @Override
    public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName, HTableDescriptor hTableDescriptor) throws IOException {

    }

    @Override
    public void preModifyTableHandler(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName, HTableDescriptor hTableDescriptor) throws IOException {

    }

    @Override
    public void preAddColumn(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName, HColumnDescriptor hColumnDescriptor) throws IOException {

    }

    @Override
    public void preAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName, HColumnDescriptor hColumnDescriptor) throws IOException {

    }

    @Override
    public void preModifyColumn(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName, HColumnDescriptor hColumnDescriptor) throws IOException {

    }

    @Override
    public void preModifyColumnHandler(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName, HColumnDescriptor hColumnDescriptor) throws IOException {

    }

    @Override
    public void preDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName, byte[] bytes) throws IOException {

    }

    @Override
    public void preDeleteColumnHandler(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName, byte[] bytes) throws IOException {

    }

    @Override
    public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName) throws IOException {

    }

    @Override
    public void preEnableTableHandler(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName) throws IOException {

    }

    @Override
    public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName) throws IOException {

    }

    @Override
    public void preDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName) throws IOException {

    }

    @Override
    public void preMove(ObserverContext<MasterCoprocessorEnvironment> observerContext, HRegionInfo hRegionInfo, ServerName serverName, ServerName serverName1) throws IOException {

    }


    @Override
    public void preListProcedures(ObserverContext<MasterCoprocessorEnvironment> observerContext) throws IOException {

    }

    @Override
    public void preAssign(ObserverContext<MasterCoprocessorEnvironment> observerContext, HRegionInfo hRegionInfo) throws IOException {

    }

    @Override
    public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> observerContext, HRegionInfo hRegionInfo, boolean b) throws IOException {

    }

    @Override
    public void preRegionOffline(ObserverContext<MasterCoprocessorEnvironment> observerContext, HRegionInfo hRegionInfo) throws IOException {

    }

    @Override
    public void preBalance(ObserverContext<MasterCoprocessorEnvironment> observerContext) throws IOException {

    }

    @Override
    public boolean preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> observerContext, boolean b) throws IOException {
        return b;
    }

    @Override
    public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> observerContext) throws IOException {

    }

    @Override
    public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> observerContext) throws IOException {

    }

    @Override
    public void preMasterInitialization(ObserverContext<MasterCoprocessorEnvironment> observerContext) throws IOException {

    }

    @Override
    public void preSnapshot(ObserverContext<MasterCoprocessorEnvironment> observerContext, SnapshotDescription snapshotDescription, HTableDescriptor hTableDescriptor) throws IOException {

    }

    @Override
    public void preListSnapshot(ObserverContext<MasterCoprocessorEnvironment> observerContext, SnapshotDescription snapshotDescription) throws IOException {

    }

    @Override
    public void preCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> observerContext, SnapshotDescription snapshotDescription, HTableDescriptor hTableDescriptor) throws IOException {

    }

    @Override
    public void preRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> observerContext, SnapshotDescription snapshotDescription, HTableDescriptor hTableDescriptor) throws IOException {

    }

    @Override
    public void preDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> observerContext, SnapshotDescription snapshotDescription) throws IOException {

    }

    @Override
    public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> observerContext, List<TableName> list, List<HTableDescriptor> list1) throws IOException {

    }

    @Override
    public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> observerContext, List<TableName> list, List<HTableDescriptor> list1, String s) throws IOException {

    }

    @Override
    public void preGetTableNames(ObserverContext<MasterCoprocessorEnvironment> observerContext, List<HTableDescriptor> list, String s) throws IOException {

    }

    @Override
    public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> observerContext, NamespaceDescriptor namespaceDescriptor) throws IOException {

    }

    @Override
    public void preDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> observerContext, String s) throws IOException {

    }

    @Override
    public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> observerContext, NamespaceDescriptor namespaceDescriptor) throws IOException {

    }

    @Override
    public void preGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> observerContext, String s) throws IOException {

    }

    @Override
    public void preListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> observerContext, List<NamespaceDescriptor> list) throws IOException {

    }

    @Override
    public void preTableFlush(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName) throws IOException {

    }

    @Override
    public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> observerContext, String s, Quotas quotas) throws IOException {

    }

    @Override
    public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> observerContext, String s, TableName tableName, Quotas quotas) throws IOException {

    }

    @Override
    public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> observerContext, String s, String s1, Quotas quotas) throws IOException {

    }

    @Override
    public void preSetTableQuota(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName, Quotas quotas) throws IOException {

    }

    @Override
    public void preSetNamespaceQuota(ObserverContext<MasterCoprocessorEnvironment> observerContext, String s, Quotas quotas) throws IOException {

    }

    @Override
    public void start(CoprocessorEnvironment coprocessorEnvironment) throws IOException {

    }

    @Override
    public void stop(CoprocessorEnvironment coprocessorEnvironment) throws IOException {

    }

    @Override
    public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> observerContext, List<HTableDescriptor> list) throws IOException {

    }

    @Override
    public void postBalance(ObserverContext<MasterCoprocessorEnvironment> observerContext, List<RegionPlan> list) throws IOException {

     }

    @Override
    public void postBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> observerContext, boolean b, boolean b1) throws IOException {

    }

    @Override
    public void postGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> observerContext, NamespaceDescriptor namespaceDescriptor) throws IOException {

    }

    @Override
    public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> observerContext) throws IOException {

    }

    @Override
    public void postSnapshot(ObserverContext<MasterCoprocessorEnvironment> observerContext, SnapshotDescription snapshotDescription, HTableDescriptor hTableDescriptor) throws IOException {

    }

    @Override
    public void postSetNamespaceQuota(ObserverContext<MasterCoprocessorEnvironment> observerContext, String s, Quotas quotas) throws IOException {

    }

    @Override
    public void postAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> observerContext) throws IOException {

    }

    @Override
    public void postListProcedures(ObserverContext<MasterCoprocessorEnvironment> observerContext, List<ProcedureInfo> list) throws IOException {

    }

    @Override
    public void postCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> observerContext, HTableDescriptor hTableDescriptor, HRegionInfo[] hRegionInfos) throws IOException {

    }

    @Override
    public void postDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName) throws IOException {

    }

    @Override
    public void postTruncateTable(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName) throws IOException {

    }

    @Override
    public void postTruncateTableHandler(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName) throws IOException {

    }

    @Override
    public void postModifyTableHandler(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName, HTableDescriptor hTableDescriptor) throws IOException {

    }

    @Override
    public void postAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName, HColumnDescriptor hColumnDescriptor) throws IOException {

    }

    @Override
    public void postModifyColumnHandler(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName, HColumnDescriptor hColumnDescriptor) throws IOException {

    }

    @Override
    public void postDeleteColumnHandler(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName, byte[] bytes) throws IOException {

    }
    @Override
    public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName) throws IOException {

    }

    @Override
    public void postEnableTableHandler(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName) throws IOException {

    }

    @Override
    public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName) throws IOException {

    }

    @Override
    public void postDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName) throws IOException {

    }

    @Override
    public void postMove(ObserverContext<MasterCoprocessorEnvironment> observerContext, HRegionInfo hRegionInfo, ServerName serverName, ServerName serverName1) throws IOException {

    }

    @Override
    public void postAssign(ObserverContext<MasterCoprocessorEnvironment> observerContext, HRegionInfo hRegionInfo) throws IOException {

    }

    @Override
    public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> observerContext, HRegionInfo hRegionInfo, boolean b) throws IOException {

    }

    @Override
    public void postRegionOffline(ObserverContext<MasterCoprocessorEnvironment> observerContext, HRegionInfo hRegionInfo) throws IOException {

    }

    @Override
    public void postListSnapshot(ObserverContext<MasterCoprocessorEnvironment> observerContext, HBaseProtos.SnapshotDescription snapshotDescription) throws IOException {

    }

    @Override
    public void postCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> observerContext, HBaseProtos.SnapshotDescription snapshotDescription, HTableDescriptor hTableDescriptor) throws IOException {

    }

    @Override
    public void postRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> observerContext, HBaseProtos.SnapshotDescription snapshotDescription, HTableDescriptor hTableDescriptor) throws IOException {

    }

    @Override
    public void postDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> observerContext, HBaseProtos.SnapshotDescription snapshotDescription) throws IOException {

    }

    @Override
    public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> observerContext, List<TableName> list, List<HTableDescriptor> list1, String s) throws IOException {

    }

    @Override
    public void postGetTableNames(ObserverContext<MasterCoprocessorEnvironment> observerContext, List<HTableDescriptor> list, String s) throws IOException {

    }

    @Override
    public void postListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> observerContext, List<NamespaceDescriptor> list) throws IOException {

    }

    @Override
    public void postTableFlush(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName) throws IOException {

    }

    @Override
    public void postSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> observerContext, String s, QuotaProtos.Quotas quotas) throws IOException {

    }

    @Override
    public void postSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> observerContext, String s, TableName tableName, QuotaProtos.Quotas quotas) throws IOException {

    }

    @Override
    public void postSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> observerContext, String s, String s1, QuotaProtos.Quotas quotas) throws IOException {

    }

    @Override
    public void postSetTableQuota(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName, QuotaProtos.Quotas quotas) throws IOException {

    }

    @Override
    public void preOpen(ObserverContext<RegionCoprocessorEnvironment> observerContext) throws IOException {

    }

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> observerContext) {

    }

    @Override
    public void postLogReplay(ObserverContext<RegionCoprocessorEnvironment> observerContext) {

    }

    @Override
    public InternalScanner preFlushScannerOpen(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store, KeyValueScanner keyValueScanner, InternalScanner internalScanner) throws IOException {
        return null;
    }

    @Override
    public void preFlush(ObserverContext<RegionCoprocessorEnvironment> observerContext) throws IOException {

    }

    @Override
    public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store, InternalScanner internalScanner) throws IOException {
        return internalScanner;
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> observerContext) throws IOException {

    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store, StoreFile storeFile) throws IOException {

    }

    @Override
    public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store, List<StoreFile> list, CompactionRequest compactionRequest) throws IOException {

    }

    @Override
    public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store, List<StoreFile> list) throws IOException {

    }

    @Override
    public void postCompactSelection(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store, ImmutableList<StoreFile> immutableList, CompactionRequest compactionRequest) {

    }

    @Override
    public void postCompactSelection(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store, ImmutableList<StoreFile> immutableList) {

    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store, InternalScanner internalScanner, ScanType scanType, CompactionRequest compactionRequest) throws IOException {
        return internalScanner;
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store, InternalScanner internalScanner, ScanType scanType) throws IOException {
        return internalScanner;
    }

    @Override
    public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store, List<? extends KeyValueScanner> list, ScanType scanType, long l, InternalScanner internalScanner, CompactionRequest compactionRequest) throws IOException {
        return internalScanner;
    }

    @Override
    public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store, List<? extends KeyValueScanner> list, ScanType scanType, long l, InternalScanner internalScanner) throws IOException {
        return internalScanner;
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store, StoreFile storeFile, CompactionRequest compactionRequest) throws IOException {

    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store, StoreFile storeFile) throws IOException {

    }

    @Override
    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> observerContext) throws IOException {

    }

    @Override
    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes) throws IOException {

    }

    @Override
    public void postSplit(ObserverContext<RegionCoprocessorEnvironment> observerContext, Region region, Region region1) throws IOException {

    }

    @Override
    public void preSplitBeforePONR(ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes, List<Mutation> list) throws IOException {

    }

    @Override
    public void preSplitAfterPONR(ObserverContext<RegionCoprocessorEnvironment> observerContext) throws IOException {

    }

    @Override
    public void preRollBackSplit(ObserverContext<RegionCoprocessorEnvironment> observerContext) throws IOException {

    }

    @Override
    public void postRollBackSplit(ObserverContext<RegionCoprocessorEnvironment> observerContext) throws IOException {

    }

    @Override
    public void postCompleteSplit(ObserverContext<RegionCoprocessorEnvironment> observerContext) throws IOException {

    }

    @Override
    public void preClose(ObserverContext<RegionCoprocessorEnvironment> observerContext, boolean b) throws IOException {

    }

    @Override
    public void postClose(ObserverContext<RegionCoprocessorEnvironment> observerContext, boolean b) {

    }

    @Override
    public void preGetClosestRowBefore(ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes, byte[] bytes1, Result result) throws IOException {

    }

    @Override
    public void postGetClosestRowBefore(ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes, byte[] bytes1, Result result) throws IOException {

    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> observerContext, Get get, List<Cell> list) throws IOException {

    }

    @Override
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> observerContext, Get get, List<Cell> list) throws IOException {

    }

    @Override
    public boolean preExists(ObserverContext<RegionCoprocessorEnvironment> observerContext, Get get, boolean b) throws IOException {
        return b;
    }

    @Override
    public boolean postExists(ObserverContext<RegionCoprocessorEnvironment> observerContext, Get get, boolean b) throws IOException {
        return b;
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> observerContext, Put put, WALEdit walEdit, Durability durability) throws IOException {

    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> observerContext, Put put, WALEdit walEdit, Durability durability) throws IOException {

    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> observerContext, Delete delete, WALEdit walEdit, Durability durability) throws IOException {

    }

    @Override
    public void prePrepareTimeStampForDeleteVersion(ObserverContext<RegionCoprocessorEnvironment> observerContext, Mutation mutation, Cell cell, byte[] bytes, Get get) throws IOException {

    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> observerContext, Delete delete, WALEdit walEdit, Durability durability) throws IOException {

    }

    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> observerContext, MiniBatchOperationInProgress<Mutation> miniBatchOperationInProgress) throws IOException {

    }

    @Override
    public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> observerContext, MiniBatchOperationInProgress<Mutation> miniBatchOperationInProgress) throws IOException {

    }

    @Override
    public void postStartRegionOperation(ObserverContext<RegionCoprocessorEnvironment> observerContext, Region.Operation operation) throws IOException {

    }

    @Override
    public void postCloseRegionOperation(ObserverContext<RegionCoprocessorEnvironment> observerContext, Region.Operation operation) throws IOException {

    }

    @Override
    public void postBatchMutateIndispensably(ObserverContext<RegionCoprocessorEnvironment> observerContext, MiniBatchOperationInProgress<Mutation> miniBatchOperationInProgress, boolean b) throws IOException {

    }

    @Override
    public boolean preCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes, byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp, ByteArrayComparable byteArrayComparable, Put put, boolean b) throws IOException {
        return b;
    }

    @Override
    public boolean preCheckAndPutAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes, byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp, ByteArrayComparable byteArrayComparable, Put put, boolean b) throws IOException {
        return false;
    }

    @Override
    public boolean postCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes, byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp, ByteArrayComparable byteArrayComparable, Put put, boolean b) throws IOException {
        return b;
    }

    @Override
    public boolean preCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes, byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp, ByteArrayComparable byteArrayComparable, Delete delete, boolean b) throws IOException {
        return b;
    }

    @Override
    public boolean preCheckAndDeleteAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes, byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp, ByteArrayComparable byteArrayComparable, Delete delete, boolean b) throws IOException {
        return b;
    }

    @Override
    public boolean postCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes, byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp, ByteArrayComparable byteArrayComparable, Delete delete, boolean b) throws IOException {
        return false;
    }

    @Override
    public long preIncrementColumnValue(ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes, byte[] bytes1, byte[] bytes2, long l, boolean b) throws IOException {
        return l;
    }

    @Override
    public long postIncrementColumnValue(ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes, byte[] bytes1, byte[] bytes2, long l, boolean b, long l1) throws IOException {
        return l;
    }

    @Override
    public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> observerContext, Append append) throws IOException {
        return null;
    }

    @Override
    public Result preAppendAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> observerContext, Append append) throws IOException {
        return null;
    }

    @Override
    public Result postAppend(ObserverContext<RegionCoprocessorEnvironment> observerContext, Append append, Result result) throws IOException {
        return result;
    }

    @Override
    public Result preIncrement(ObserverContext<RegionCoprocessorEnvironment> observerContext, Increment increment) throws IOException {
        return null;
    }

    @Override
    public Result preIncrementAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> observerContext, Increment increment) throws IOException {
        return null;
    }

    @Override
    public Result postIncrement(ObserverContext<RegionCoprocessorEnvironment> observerContext, Increment increment, Result result) throws IOException {
        return result;
    }

    @Override
    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> observerContext, Scan scan, RegionScanner regionScanner) throws IOException {
        return regionScanner;
    }

    @Override
    public KeyValueScanner preStoreScannerOpen(ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store, Scan scan, NavigableSet<byte[]> navigableSet, KeyValueScanner keyValueScanner) throws IOException {
        return keyValueScanner;
    }

    @Override
    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> observerContext, Scan scan, RegionScanner regionScanner) throws IOException {
        return regionScanner;
    }

    @Override
    public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> observerContext, InternalScanner internalScanner, List<Result> list, int i, boolean b) throws IOException {
        return b;
    }

    @Override
    public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> observerContext, InternalScanner internalScanner, List<Result> list, int i, boolean b) throws IOException {
        return b;
    }

    @Override
    public boolean postScannerFilterRow(ObserverContext<RegionCoprocessorEnvironment> observerContext, InternalScanner internalScanner, byte[] bytes, int i, short i1, boolean b) throws IOException {
        return b;
    }

    @Override
    public void preScannerClose(ObserverContext<RegionCoprocessorEnvironment> observerContext, InternalScanner internalScanner) throws IOException {

    }

    @Override
    public void postScannerClose(ObserverContext<RegionCoprocessorEnvironment> observerContext, InternalScanner internalScanner) throws IOException {

    }

    @Override
    public void preWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> observerContext, HRegionInfo hRegionInfo, WALKey walKey, WALEdit walEdit) throws IOException {

    }

    @Override
    public void preWALRestore(ObserverContext<RegionCoprocessorEnvironment> observerContext, HRegionInfo hRegionInfo, HLogKey hLogKey, WALEdit walEdit) throws IOException {

    }

    @Override
    public void postWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> observerContext, HRegionInfo hRegionInfo, WALKey walKey, WALEdit walEdit) throws IOException {

    }

    @Override
    public void postWALRestore(ObserverContext<RegionCoprocessorEnvironment> observerContext, HRegionInfo hRegionInfo, HLogKey hLogKey, WALEdit walEdit) throws IOException {

    }

    @Override
    public void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> observerContext, List<Pair<byte[], String>> list) throws IOException {

    }

    @Override
    public boolean postBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> observerContext, List<Pair<byte[], String>> list, boolean b) throws IOException {
        return b;
    }

    @Override
    public StoreFile.Reader preStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> observerContext, FileSystem fileSystem, Path path, FSDataInputStreamWrapper fsDataInputStreamWrapper, long l, CacheConfig cacheConfig, Reference reference, StoreFile.Reader reader) throws IOException {
        return reader;
    }

    @Override
    public StoreFile.Reader postStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> observerContext, FileSystem fileSystem, Path path, FSDataInputStreamWrapper fsDataInputStreamWrapper, long l, CacheConfig cacheConfig, Reference reference, StoreFile.Reader reader) throws IOException {
        return reader;
    }

    @Override
    public Cell postMutationBeforeWAL(ObserverContext<RegionCoprocessorEnvironment> observerContext, MutationType mutationType, Mutation mutation, Cell cell, Cell cell1) throws IOException {
        return cell;
    }

    @Override
    public DeleteTracker postInstantiateDeleteTracker(ObserverContext<RegionCoprocessorEnvironment> observerContext, DeleteTracker deleteTracker) throws IOException {
        return deleteTracker;
    }

    @Override
    public void preStopRegionServer(ObserverContext<RegionServerCoprocessorEnvironment> observerContext) throws IOException {

    }

    @Override
    public void preMerge(ObserverContext<RegionServerCoprocessorEnvironment> observerContext, Region region, Region region1) throws IOException {

    }

    @Override
    public void postMerge(ObserverContext<RegionServerCoprocessorEnvironment> observerContext, Region region, Region region1, Region region2) throws IOException {

    }

    @Override
    public void preMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> observerContext, Region region, Region region1, @MetaMutationAnnotation List<Mutation> list) throws IOException {

    }

    @Override
    public void postMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> observerContext, Region region, Region region1, Region region2) throws IOException {

    }

    @Override
    public void preRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> observerContext, Region region, Region region1) throws IOException {

    }

    @Override
    public void postRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> observerContext, Region region, Region region1) throws IOException {

    }

    @Override
    public void preRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> observerContext) throws IOException {

    }

    @Override
    public void postRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> observerContext) throws IOException {

    }

    @Override
    public ReplicationEndpoint postCreateReplicationEndPoint(ObserverContext<RegionServerCoprocessorEnvironment> observerContext, ReplicationEndpoint replicationEndpoint) {
        return null;
    }

    @Override
    public void preReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> observerContext, List<AdminProtos.WALEntry> list, CellScanner cellScanner) throws IOException {

    }

    @Override
    public void postReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> observerContext, List<AdminProtos.WALEntry> list, CellScanner cellScanner) throws IOException {

    }

    @Override
    public void prePrepareBulkLoad(ObserverContext<RegionCoprocessorEnvironment> observerContext, SecureBulkLoadProtos.PrepareBulkLoadRequest prepareBulkLoadRequest) throws IOException {

    }

    @Override
    public void preCleanupBulkLoad(ObserverContext<RegionCoprocessorEnvironment> observerContext, SecureBulkLoadProtos.CleanupBulkLoadRequest cleanupBulkLoadRequest) throws IOException {

    }

    @Override
    public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> observerContext, HTableDescriptor hTableDescriptor, HRegionInfo[] hRegionInfos) throws IOException {

    }



    @Override
    public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName) throws IOException {

    }

    @Override
    public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName, HTableDescriptor hTableDescriptor) throws IOException {

    }

    @Override
    public void postAddColumn(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName, HColumnDescriptor hColumnDescriptor) throws IOException {

    }

    @Override
    public void postModifyColumn(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName, HColumnDescriptor hColumnDescriptor) throws IOException {

    }

    @Override
    public void postDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> observerContext, TableName tableName, byte[] bytes) throws IOException {

    }

    @Override
    public void postCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> observerContext, NamespaceDescriptor namespaceDescriptor) throws IOException {

    }

    @Override
    public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> observerContext, String s) throws IOException {

    }

    @Override
    public void postModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> observerContext, NamespaceDescriptor namespaceDescriptor) throws IOException {

    }

    @Override
    public void preAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> observerContext, ProcedureExecutor<MasterProcedureEnv> procedureExecutor, long l) throws IOException {

    }
}
