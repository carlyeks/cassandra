/*
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

package org.apache.cassandra.utils.concurrent;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.NotImplementedException;

import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Allows Clustering-level resolution of locks.
 * <p>
 * A ClusteringAwareLock is at a partition level
 */
public class ClusteringAwareLock
{
    public interface Unlockable
    {
        boolean acquired();

        void unlock();
    }

    private static final int STRIPES = 1024;

    private static final Unlockable empty = new Unlockable()
    {
        public boolean acquired()
        {
            return false;
        }

        public void unlock()
        {

        }
    };

    private final ByteBuffer partitionKey;

    private final Lock partitionLevelLock;
    private final Map<Integer, Lock> clusteringLocks;

    public ClusteringAwareLock(ByteBuffer partitionKey)
    {
        this.partitionKey = partitionKey;
        this.partitionLevelLock = new ReentrantLock();
        this.clusteringLocks = new HashMap<>();
    }

    private static boolean hasExpired(long expiryNanos)
    {
        return System.nanoTime() > expiryNanos;
    }

    private static Unlockable unlock(Set<Lock> unlockNow, final Collection<Lock> unlockLater)
    {
        for (Lock lock : unlockLater)
        {
            unlockNow.remove(lock);
        }
        return new Unlockable()
        {
            public boolean acquired()
            {
                return true;
            }

            public void unlock()
            {
                for (Lock lock : unlockLater)
                {
                    lock.unlock();
                }
            }
        };
    }

    private boolean tryAcquire(Collection<Lock> unlockNow, Lock lock, long expiry) throws InterruptedException
    {
        if (hasExpired(expiry))
            return false;

        if (lock.tryLock(System.nanoTime() - expiry, TimeUnit.NANOSECONDS))
        {
            unlockNow.add(lock);
            return true;
        }
        return false;
    }

    private boolean tryAcquire(Collection<Lock> unlockNow, Collection<Integer> tryLock, long expiry) throws InterruptedException
    {
        List<Integer> acquireOrder = new ArrayList<>();
        for (Integer toLock : tryLock)
        {
            acquireOrder.add(toLock % STRIPES);
        }

        Collections.sort(acquireOrder);
        int last = -1;
        for (int lockId : acquireOrder)
        {
            if (last == lockId) continue;

            Lock lock = clusteringLocks.computeIfAbsent(lockId, ignore -> new ReentrantLock());
            if (!tryAcquire(unlockNow, lock, expiry))
            {
                return false;
            }

            last = lockId;
        }
        return true;
    }

    private boolean tryAcquireAllLocks(Set<Lock> unlockNow, long expiry) throws InterruptedException
    {
        if (tryAcquire(unlockNow, clusteringLocks.keySet(), expiry))
        {
            clusteringLocks.clear();
            return true;
        }
        return false;
    }

    public Unlockable tryLock(long time, TimeUnit unit, PartitionUpdate update) throws InterruptedException
    {
        assert ByteBufferUtil.compareUnsigned(update.partitionKey().getKey(), partitionKey) == 0 : "Different partitions could cause collisions";
        long expiry = System.nanoTime() + unit.toNanos(time);
        Set<Lock> unlockNow = new HashSet<>();

        // We acquire the partition level lock always, since we need to ensure that we have at least that lock
        if (!tryAcquire(unlockNow, partitionLevelLock, expiry))
        {
            return empty;
        }

        try
        {
            if (!update.partitionLevelDeletion().isLive() || !update.staticRow().isEmpty())
            {
                if (!tryAcquireAllLocks(unlockNow, expiry))
                {
                    return empty;
                }
                return unlock(unlockNow, Collections.singleton(partitionLevelLock));
            }
            else
            {
                Iterator<Row> rows = update.iterator();
                Set<Integer> toAcquire = new HashSet<>();
                while (rows.hasNext())
                {
                    Row next = rows.next();
                    toAcquire.add(next.clustering().hashCode());
                }
                Collection<Lock> clusteringLocks = new ArrayList<>();
                Collection<Lock> captureAdd = new DualAddCollection<>(unlockNow, clusteringLocks);
                if (!tryAcquire(captureAdd, toAcquire, expiry))
                {
                    return empty;
                }
                return unlock(unlockNow, clusteringLocks);
            }
        }
        finally
        {
            // We remove the locks that will be unlocked later in unlock(); the locks left should be released now.
            for (Lock lock : unlockNow)
                lock.unlock();
        }
    }

    private static class DualAddCollection<T> implements Collection<T>
    {
        Collection<Collection<T>> collections;

        public DualAddCollection(final Collection<T> first, final Collection<T> second)
        {
            this.collections = new ArrayList<Collection<T>>()
            {{
                add(first);
                add(second);
            }};
        }

        public int size()
        {
            throw new NotImplementedException();
        }

        public boolean isEmpty()
        {
            throw new NotImplementedException();
        }

        public boolean contains(Object o)
        {
            throw new NotImplementedException();
        }

        public Iterator<T> iterator()
        {
            throw new NotImplementedException();
        }

        public Object[] toArray()
        {
            throw new NotImplementedException();
        }

        public <T1> T1[] toArray(T1[] a)
        {
            throw new NotImplementedException();
        }

        public boolean add(T t)
        {
            for (Collection<T> collection : collections)
            {
                collection.add(t);
            }
            return true;
        }

        public boolean remove(Object o)
        {
            throw new NotImplementedException();
        }

        public boolean containsAll(Collection<?> c)
        {
            throw new NotImplementedException();
        }

        public boolean addAll(Collection<? extends T> c)
        {
            throw new NotImplementedException();
        }

        public boolean removeAll(Collection<?> c)
        {
            throw new NotImplementedException();
        }

        public boolean retainAll(Collection<?> c)
        {
            throw new NotImplementedException();
        }

        public void clear()
        {
            throw new NotImplementedException();
        }
    }
}
