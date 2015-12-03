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
package com.thinkaurelius.titan.diskstorage.locking;

import com.thinkaurelius.titan.diskstorage.hbase.HBaseTransaction;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.diskstorage.util.time.Timestamps;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

public class LocalLockMediatorTest {

    private static final String LOCK_NAMESPACE = "test";
    private static final StaticBuffer LOCK_ROW = StaticArrayBuffer.of(new byte[]{1});
    private static final StaticBuffer LOCK_COL = StaticArrayBuffer.of(new byte[]{1});
    private static final KeyColumn kc = new KeyColumn(LOCK_ROW, LOCK_COL);
    private static final HBaseTransaction mockTx1 = Mockito.mock(HBaseTransaction.class);
    private static final HBaseTransaction mockTx2 = Mockito.mock(HBaseTransaction.class);

    @Test
    public void testLock() throws InterruptedException {
        TimestampProvider times = Timestamps.MICRO;
        LocalLockMediator<HBaseTransaction> llm =
            new LocalLockMediator<HBaseTransaction>(LOCK_NAMESPACE, times);

        //Expire immediately
        Assert.assertTrue(llm.lock(kc, mockTx1, times.getTime(0, TimeUnit.NANOSECONDS)));
        Assert.assertTrue(llm.lock(kc, mockTx2, times.getTime(Long.MAX_VALUE, TimeUnit.NANOSECONDS)));

        llm = new LocalLockMediator<HBaseTransaction>(LOCK_NAMESPACE, times);

        //Expire later
        Assert.assertTrue(llm.lock(kc, mockTx1, times.getTime(Long.MAX_VALUE, TimeUnit.NANOSECONDS)));
        //So second lock should fail on same keyCol
        Assert.assertFalse(llm.lock(kc, mockTx2, times.getTime(Long.MAX_VALUE, TimeUnit.NANOSECONDS)));

        //Unlock
        Assert.assertTrue(llm.unlock(kc, mockTx1));
        //Now locking should succeed
        Assert.assertTrue(llm.lock(kc, mockTx2, times.getTime(Long.MAX_VALUE, TimeUnit.NANOSECONDS)));
    }
}
