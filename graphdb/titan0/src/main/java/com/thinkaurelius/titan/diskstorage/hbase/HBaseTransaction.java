/*
 * Copyright 2012-2013 Aurelius LLC
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediator;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * This class overrides and adds nothing compared with
 * {@link com.thinkaurelius.titan.diskstorage.locking.consistentkey.ExpectedValueCheckingTransaction}; however, it creates a transaction type specific
 * to HBase, which lets us check for user errors like passing a Cassandra
 * transaction into a HBase method.
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class HBaseTransaction extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(HBaseTransaction.class);

    LocalLockMediator<StoreTransaction> llm;

    Set<KeyColumn> keyColumnLocks = new LinkedHashSet<>();

    public HBaseTransaction(final BaseTransactionConfig config, LocalLockMediator<StoreTransaction> llm) {
        super(config);
        this.llm = llm;
    }

    @Override
    public synchronized void rollback() throws BackendException {
        super.rollback();
        log.debug("Rolled back transaction");
        deleteAllLocks();
    }

    @Override
    public synchronized void commit() throws BackendException {
        super.commit();
        log.debug("Committed transaction");
        deleteAllLocks();
    }

    public void updateLocks(KeyColumn lockID, StaticBuffer expectedValue) {
        keyColumnLocks.add(lockID);
    }

    private void deleteAllLocks() {
        for(KeyColumn kc : keyColumnLocks) {
            log.debug("Removed lock {} ", kc);
            llm.unlock(kc, this);
        }
    }
}
