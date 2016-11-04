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

package org.apache.cassandra.db.compaction;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;

class CompactionTaskStats
{
    LongStats partitionRows;
    LongStats partitionCells;
    LongStats partitionDataSize;
    LongStats partitionTombstones;

    long partitionCount = 0L;
    private long startMillis;
    private long startNanos;

    CompactionTaskStats()
    {
        startMillis = System.currentTimeMillis();
        startNanos = System.nanoTime();
        partitionRows = new LongStats();
        partitionCells = new LongStats();
        partitionDataSize = new LongStats();
        partitionTombstones = new LongStats();
    }

    PartitionStats startPartition()
    {
        return new PartitionStats();
    }

    class PartitionStats implements UnfilteredRowIterators.MergeListener
    {
        long cqlRows = 0L;
        long cqlRowsWithMerge = 0L;
        long cqlCells = 0L;
        long cqlMergedCells = 0L;
        long cqlDataSize = 0L;
        long rangeTombstones = 0L;

        public void onMergedPartitionLevelDeletion(DeletionTime mergedDeletion, DeletionTime[] versions)
        {

        }

        public void onMergedRows(Row merged, Row[] versions)
        {
            cqlRows++;
            long finalSize = merged.size();
            cqlCells += finalSize;
            cqlDataSize += merged.dataSize();

            long mergedCells = 0L;
            for (Row version : versions)
                if (version != null)
                    mergedCells += version.size();
            cqlMergedCells += mergedCells;

            if (mergedCells == finalSize)
                cqlRowsWithMerge++;
        }

        public void onMergedRangeTombstoneMarkers(RangeTombstoneMarker merged, RangeTombstoneMarker[] versions)
        {
            rangeTombstones++;
        }

        public void close()
        {
            partitionCount++;
            partitionRows.addOne(cqlRows);
            partitionDataSize.addOne(cqlDataSize);
            partitionCells.addOne(cqlCells);
            partitionTombstones.addOne(rangeTombstones);
        }
    }

    static class LongStats
    {
        private long min = Long.MAX_VALUE;
        private long max = Long.MIN_VALUE;
        private long count = 0;
        private long sum = 0L;

        void addOne(long value)
        {
            count++;
            sum += value;
            if (min > value)
                min = value;
            if (max < value)
                max = value;
        }

        long min()
        {
            return min;
        }

        long max()
        {
            return max;
        }

        long count()
        {
            return count;
        }

        long sum()
        {
            return sum;
        }

        double rawMean()
        {
            return ((double) sum) / count;
        }

        long mean()
        {
            return (long) Math.ceil(rawMean());
        }
    }
}
