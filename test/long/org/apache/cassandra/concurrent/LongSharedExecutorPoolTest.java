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
package org.apache.cassandra.concurrent;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

public class LongSharedExecutorPoolTest
{

    private static final class WaitTask implements Runnable
    {
        final long nanos;

        private WaitTask(long nanos)
        {
            this.nanos = nanos;
        }

        public void run()
        {
            LockSupport.parkNanos(nanos);
        }
    }

    private static final class Result implements Comparable<Result>
    {
        final Future<?> future;
        final long forecastedCompletion;

        private Result(Future<?> future, long forecastedCompletion)
        {
            this.future = future;
            this.forecastedCompletion = forecastedCompletion;
        }

        public int compareTo(Result that)
        {
            int c = Long.compare(this.forecastedCompletion, that.forecastedCompletion);
            if (c != 0)
                return c;
            c = Integer.compare(this.hashCode(), that.hashCode());
            if (c != 0)
                return c;
            return Integer.compare(this.future.hashCode(), that.future.hashCode());
        }
    }

    private static final class Batch implements Comparable<Batch>
    {
        final TreeSet<Result> results;
        final long timeout;
        final int executorIndex;

        private Batch(TreeSet<Result> results, long timeout, int executorIndex)
        {
            this.results = results;
            this.timeout = timeout;
            this.executorIndex = executorIndex;
        }

        public int compareTo(Batch that)
        {
            int c = Long.compare(this.timeout, that.timeout);
            if (c != 0)
                return c;
            c = Integer.compare(this.results.size(), that.results.size());
            if (c != 0)
                return c;
            return Integer.compare(this.hashCode(), that.hashCode());
        }
    }


}
