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

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.util.time.Timepoint;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ExpectedValueCheckingTransaction;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * This class resolves lock contention between two transactions on the same JVM.
 * <p/>
 * This is not just an optimization to reduce network traffic. Locks written by
 * Titan to a distributed key-value store contain an identifier, the "Rid",
 * which is unique only to the process level. The Rid can't tell which
 * transaction in a process holds any given lock. This class prevents two
 * transactions in a single process from concurrently writing the same lock to a
 * distributed key-value store.
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */

public class LocalLockMediator<T> {

    private static final Logger log = LoggerFactory
        .getLogger(LocalLockMediator.class);

    /**
     * Namespace for which this mediator is responsible
     *
     * @see LocalLockMediatorProvider
     */
    private final String name;

    private final TimestampProvider times;

    private DelayQueue<ExpirableKeyColumn> expiryQueue = new DelayQueue<>();

    private ExecutorService lockCleanerService = Executors.newFixedThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = Executors.defaultThreadFactory().newThread(runnable);
            thread.setDaemon(true);
            return thread;
        }
    });



    /**
     * Maps a ({@code key}, {@code column}) pair to the local transaction
     * holding a lock on that pair. Values in this map may have already expired
     * according to {@link AuditRecord#expires}, in which case the lock should
     * be considered invalid.
     */
    private final ConcurrentHashMap<KeyColumn, AuditRecord<T>> locks = new ConcurrentHashMap<KeyColumn, AuditRecord<T>>();

    public LocalLockMediator(String name, TimestampProvider times) {
        this.name = name;
        this.times = times;

        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(times);
        lockCleanerService.submit(new LockCleaner());
    }

    /**
     * Acquire the lock specified by {@code kc}.
     * <p/>
     * <p/>
     * For any particular key-column, whatever value of {@code requestor} is
     * passed to this method must also be passed to the associated later call to
     * {@link #unlock(KeyColumn, ExpectedValueCheckingTransaction)}.
     * <p/>
     * If some requestor {@code r} calls this method on a KeyColumn {@code k}
     * and this method returns true, then subsequent calls to this method by
     * {@code r} on {@code l} merely attempt to update the {@code expiresAt}
     * timestamp. This differs from typical lock reentrance: multiple successful
     * calls to this method do not require an equal number of calls to
     * {@code #unlock()}. One {@code #unlock()} call is enough, no matter how
     * many times a {@code requestor} called {@code lock} beforehand. Note that
     * updating the timestamp may fail, in which case the lock is considered to
     * have expired and the calling context should assume it no longer holds the
     * lock specified by {@code kc}.
     * <p/>
     * The number of nanoseconds elapsed since the UNIX Epoch is not readily
     * available within the JVM. When reckoning expiration times, this method
     * uses the approximation implemented by
     * {@link com.thinkaurelius.titan.diskstorage.util.NanoTime#getApproxNSSinceEpoch(false)}.
     * <p/>
     * The current implementation of this method returns true when given an
     * {@code expiresAt} argument in the past. Future implementations may return
     * false instead.
     *
     * @param kc        lock identifier
     * @param requestor the object locking {@code kc}
     * @param expires   instant at which this lock will automatically expire
     * @return true if the lock is acquired, false if it was not acquired
     */
    public boolean lock(KeyColumn kc, T requestor, Timepoint expires) {
        assert null != kc;
        assert null != requestor;

        AuditRecord<T> audit = new AuditRecord<T>(requestor, expires);
        AuditRecord<T> inmap = locks.putIfAbsent(kc, audit);

        boolean success = false;

        if (null == inmap) {
            // Uncontended lock succeeded
            if (log.isTraceEnabled()) {
                log.trace("New local lock created: {} namespace={} txn={}",
                    new Object[]{kc, name, requestor});
            }
            success = true;
        } else if (inmap.equals(audit)) {
            // requestor has already locked kc; update expiresAt
            success = locks.replace(kc, inmap, audit);
            if (log.isTraceEnabled()) {
                if (success) {
                    log.trace(
                        "Updated local lock expiration: {} namespace={} txn={} oldexp={} newexp={}",
                        new Object[]{kc, name, requestor, inmap.expires,
                            audit.expires});
                } else {
                    log.trace(
                        "Failed to update local lock expiration: {} namespace={} txn={} oldexp={} newexp={}",
                        new Object[]{kc, name, requestor, inmap.expires,
                            audit.expires});
                }
            }
        } else if (0 > inmap.expires.compareTo(times.getTime())) {
            // the recorded lock has expired; replace it
            success = locks.replace(kc, inmap, audit);
            if (log.isTraceEnabled()) {
                log.trace(
                    "Discarding expired lock: {} namespace={} txn={} expired={}",
                    new Object[]{kc, name, inmap.holder, inmap.expires});
            }
        } else {
            // we lost to a valid lock
            if (log.isTraceEnabled()) {
                log.trace(
                    "Local lock failed: {} namespace={} txn={} (already owned by {})",
                    new Object[]{kc, name, requestor, inmap});
            }
        }

        if (success) {
            expiryQueue.add(new ExpirableKeyColumn(kc, expires));
        }
        return success;
    }

    /**
     * Release the lock specified by {@code kc} and which was previously
     * locked by {@code requestor}, if it is possible to release it.
     *
     * @param kc        lock identifier
     * @param requestor the object which previously locked {@code kc}
     */
    public boolean unlock(KeyColumn kc, T requestor) {

        if (!locks.containsKey(kc)) {
            log.info("Local unlock failed: no locks found for {}", kc);
            return false;
        }

        AuditRecord<T> unlocker = new AuditRecord<T>(requestor, null);

        AuditRecord<T> holder = locks.get(kc);

        if (!holder.equals(unlocker)) {
            log.error("Local unlock of {} by {} failed: it is held by {}",
                new Object[]{kc, unlocker, holder});
            return false;
        }

        boolean removed = locks.remove(kc, unlocker);

        if (removed) {
            expiryQueue.remove(kc);
            if (log.isTraceEnabled()) {
                log.trace("Local unlock succeeded: {} namespace={} txn={}",
                    new Object[]{kc, name, requestor});
            }
        } else {
            log.warn("Local unlock warning: lock record for {} disappeared "
                + "during removal; this suggests the lock either expired "
                + "while we were removing it, or that it was erroneously "
                + "unlocked multiple times.", kc);
        }

        // Even if !removed, we're finished unlocking, so return true
        return true;
    }

    public String toString() {
        return "LocalLockMediator [" + name + ",  ~" + locks.size()
            + " current locks]";
    }

    /**
     * A record containing the local transaction that holds a lock and the
     * lock's expiration time.
     */
    private static class AuditRecord<T> {

        /**
         * The local transaction that holds/held the lock.
         */
        private final T holder;
        /**
         * The expiration time of a the lock.
         */
        private final Timepoint expires;
        /**
         * Cached hashCode.
         */
        private int hashCode;

        private AuditRecord(T holder, Timepoint expires) {
            this.holder = holder;
            this.expires = expires;
        }

        /**
         * This implementation depends only on the lock holder and not on the
         * lock expiration time.
         */
        @Override
        public int hashCode() {
            if (0 == hashCode)
                hashCode = holder.hashCode();

            return hashCode;
        }

        /**
         * This implementation depends only on the lock holder and not on the
         * lock expiration time.
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            /*
             * This warning suppression is harmless because we are only going to
             * call other.holder.equals(...), and since equals(...) is part of
             * Object, it is guaranteed to be defined no matter the concrete
             * type of parameter T.
             */
            @SuppressWarnings("rawtypes")
            AuditRecord other = (AuditRecord) obj;
            if (holder == null) {
                if (other.holder != null)
                    return false;
            } else if (!holder.equals(other.holder))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "AuditRecord [txn=" + holder + ", expires=" + expires + "]";
        }

    }

    private class LockCleaner implements Runnable {

        @Override
        public void run() {
            try {
                while (true) {
                    log.debug("Lock Cleaner service started");
                    ExpirableKeyColumn lock = expiryQueue.take();
                    log.debug("Expiring key column " + lock.getKeyColumn());
                    locks.remove(lock.getKeyColumn());
                }
            } catch (InterruptedException e) {
                log.debug("Received interrupt. Exiting");
            }
        }
    }

    private static class ExpirableKeyColumn implements Delayed {

        private Timepoint expiryTime;
        private KeyColumn kc;

        public ExpirableKeyColumn(KeyColumn keyColumn, Timepoint expiryTime) {
            this.kc = keyColumn;
            this.expiryTime = expiryTime;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return expiryTime.getTimestamp(TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            if (this.expiryTime.getTimestamp(TimeUnit.NANOSECONDS) < ((ExpirableKeyColumn) o).expiryTime.getTimestamp(TimeUnit.NANOSECONDS)) {
                return -1;
            }
            if (this.expiryTime.getTimestamp(TimeUnit.NANOSECONDS) > ((ExpirableKeyColumn) o).expiryTime.getTimestamp(TimeUnit.NANOSECONDS)) {
                return 1;
            }
            return 0;
        }

        public KeyColumn getKeyColumn() {
            return kc;
        }
    }
}
