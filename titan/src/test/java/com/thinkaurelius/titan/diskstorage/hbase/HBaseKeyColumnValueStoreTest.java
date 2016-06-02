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

package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.EntryMetaData;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediator;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;
import com.thinkaurelius.titan.diskstorage.util.time.Timepoint;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

public class HBaseKeyColumnValueStoreTest {

    @Mock
    HBaseStoreManager storeManager;

    @Mock
    ConnectionMask connectionMask;

    @Mock
    LocalLockMediator localLockMediator;

    @Mock
    StaticBuffer key;

    @Mock
    StaticBuffer column;

    @Mock
    StaticBuffer expectedValue;

    @Mock
    HBaseTransaction transaction;

    @Mock
    Configuration storageConfig;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldSucceedInLockingIfLockMediatorSucceeds() throws BackendException {

        when(storeManager.getMetaDataSchema("hbase")).thenReturn(new EntryMetaData[] {EntryMetaData.TIMESTAMP});
        when(storeManager.getStorageConfig()).thenReturn(storageConfig);
        when(storageConfig.get(GraphDatabaseConfiguration.LOCK_EXPIRE)).thenReturn(
                new StandardDuration(300L, TimeUnit.MILLISECONDS));
        when(storageConfig.get(GraphDatabaseConfiguration.LOCK_WAIT)).thenReturn(
                new StandardDuration(10L, TimeUnit.MILLISECONDS));
        when(storageConfig.get(GraphDatabaseConfiguration.LOCK_RETRY)).thenReturn(3);
        KeyColumn lockID = new KeyColumn(key, column);
        when(localLockMediator.lock(eq(lockID), eq(transaction), any(Timepoint.class))).
                thenReturn(true);

        HBaseKeyColumnValueStore hBaseKeyColumnValueStore =
                new HBaseKeyColumnValueStore(storeManager, connectionMask, "titan", "e", "hbase", localLockMediator);
        hBaseKeyColumnValueStore.acquireLock(key, column, expectedValue, transaction);

        verify(transaction).updateLocks(lockID, expectedValue);
        verify(localLockMediator, times(1)).lock(eq(lockID), eq(transaction), any(Timepoint.class));
    }

    @Test
    public void shouldRetryRightNumberOfTimesIfLockMediationFails() throws BackendException {
        when(storeManager.getMetaDataSchema("hbase")).thenReturn(new EntryMetaData[] {EntryMetaData.TIMESTAMP});
        when(storeManager.getStorageConfig()).thenReturn(storageConfig);
        when(storageConfig.get(GraphDatabaseConfiguration.LOCK_EXPIRE)).thenReturn(
                new StandardDuration(300L, TimeUnit.MILLISECONDS));
        when(storageConfig.get(GraphDatabaseConfiguration.LOCK_WAIT)).thenReturn(
                new StandardDuration(10L, TimeUnit.MILLISECONDS));
        when(storageConfig.get(GraphDatabaseConfiguration.LOCK_RETRY)).thenReturn(3);
        KeyColumn lockID = new KeyColumn(key, column);
        when(localLockMediator.lock(eq(lockID), eq(transaction), any(Timepoint.class))).
                thenReturn(false).thenReturn(false).thenReturn(true);

        HBaseKeyColumnValueStore hBaseKeyColumnValueStore =
                new HBaseKeyColumnValueStore(storeManager, connectionMask, "titan", "e", "hbase", localLockMediator);
        hBaseKeyColumnValueStore.acquireLock(key, column, expectedValue, transaction);

        verify(transaction).updateLocks(lockID, expectedValue);
        verify(localLockMediator, times(3)).lock(eq(lockID), eq(transaction), any(Timepoint.class));
    }

    @Test(expectedExceptions = PermanentLockingException.class)
    public void shouldThrowExceptionAfterConfiguredRetriesIfLockMediationFails() throws BackendException {
        when(storeManager.getMetaDataSchema("hbase")).thenReturn(new EntryMetaData[] {EntryMetaData.TIMESTAMP});
        when(storeManager.getStorageConfig()).thenReturn(storageConfig);
        when(storageConfig.get(GraphDatabaseConfiguration.LOCK_EXPIRE)).thenReturn(
                new StandardDuration(300L, TimeUnit.MILLISECONDS));
        when(storageConfig.get(GraphDatabaseConfiguration.LOCK_WAIT)).thenReturn(
                new StandardDuration(10L, TimeUnit.MILLISECONDS));
        when(storageConfig.get(GraphDatabaseConfiguration.LOCK_RETRY)).thenReturn(3);
        KeyColumn lockID = new KeyColumn(key, column);
        when(localLockMediator.lock(eq(lockID), eq(transaction), any(Timepoint.class))).
                thenReturn(false).thenReturn(false).thenReturn(false);

        HBaseKeyColumnValueStore hBaseKeyColumnValueStore =
                new HBaseKeyColumnValueStore(storeManager, connectionMask, "titan", "e", "hbase", localLockMediator);
        hBaseKeyColumnValueStore.acquireLock(key, column, expectedValue, transaction);

        fail("Should fail as lock could not be acquired after 3 retries.");
    }
}
