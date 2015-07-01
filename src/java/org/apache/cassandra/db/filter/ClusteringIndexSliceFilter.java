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
package org.apache.cassandra.db.filter;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * A filter over a single partition.
 */
public class ClusteringIndexSliceFilter extends AbstractClusteringIndexFilter
{
    static final InternalDeserializer deserializer = new SliceDeserializer();

    private final Slices slices;

    public ClusteringIndexSliceFilter(Slices slices, boolean reversed)
    {
        super(reversed);
        this.slices = slices;
    }

    public Slices requestedSlices()
    {
        return slices;
    }

    public boolean selectsAllPartition()
    {
        return slices.size() == 1 && !slices.hasLowerBound() && !slices.hasUpperBound();
    }

    public boolean selects(Clustering clustering)
    {
        return slices.selects(clustering);
    }

    public ClusteringIndexSliceFilter forPaging(ClusteringComparator comparator, Clustering lastReturned, boolean inclusive)
    {
        Slices newSlices = slices.forPaging(comparator, lastReturned, inclusive, reversed);
        return slices == newSlices
             ? this
             : new ClusteringIndexSliceFilter(newSlices, reversed);
    }

    public boolean isFullyCoveredBy(CachedPartition partition)
    {
        // Partition is guaranteed to cover the whole filter if it includes the filter start and finish bounds.

        // (note that since partition is the head of a partition, to have no lower bound is ok)
        if (!slices.hasUpperBound() || partition.isEmpty())
            return false;

        return partition.metadata().comparator.compare(slices.get(slices.size() - 1).end(), partition.lastRow().clustering()) <= 0;
    }

    public boolean isHeadFilter()
    {
        return !reversed && slices.size() == 1 && !slices.hasLowerBound();
    }

    // Given another iterator, only return the rows that match this filter
    public UnfilteredRowIterator filterNotIndexed(final ColumnFilter columnFilter, UnfilteredRowIterator iterator)
    {
        final Slices.InOrderTester tester = slices.inOrderTester(reversed);

        // Note that we don't filter markers because that's a bit trickier (we don't know in advance until when
        // the range extend) and it's harmless to leave them.
        return new FilteringRowIterator(iterator)
        {
            @Override
            public FilteringRow makeRowFilter()
            {
                return FilteringRow.columnsFilteringRow(columnFilter);
            }

            @Override
            protected boolean includeRow(Row row)
            {
                return tester.includes(row.clustering());
            }

            @Override
            public boolean hasNext()
            {
                return !tester.isDone() && super.hasNext();
            }
        };
    }

    public UnfilteredRowIterator filter(SliceableUnfilteredRowIterator iterator)
    {
        // Please note that this method assumes that rows from 'iter' already have their columns filtered, i.e. that
        // they only include columns that we select.
        return slices.makeSliceIterator(iterator);
    }

    public UnfilteredRowIterator getUnfilteredRowIterator(ColumnFilter columnFilter, Partition partition)
    {
        return partition.unfilteredIterator(columnFilter, slices, reversed);
    }

    public boolean shouldInclude(SSTableReader sstable)
    {
        List<ByteBuffer> minClusteringValues = sstable.getSSTableMetadata().minClusteringValues;
        List<ByteBuffer> maxClusteringValues = sstable.getSSTableMetadata().maxClusteringValues;

        if (minClusteringValues.isEmpty() || maxClusteringValues.isEmpty())
            return true;

        return slices.intersects(minClusteringValues, maxClusteringValues);
    }

    public String toString(CFMetaData metadata)
    {
        return String.format("slice(slices=%s, reversed=%b)", slices, reversed);
    }

    public String toCQLString(CFMetaData metadata)
    {
        StringBuilder sb = new StringBuilder();

        if (!selectsAllPartition())
            sb.append(slices.toCQLString(metadata));

        appendOrderByToCQLString(metadata, sb);

        return sb.toString();
    }

    Kind kind()
    {
        return Kind.SLICE;
    }

    protected void serializeInternal(DataOutputPlus out, int version) throws IOException
    {
        Slices.serializer.serialize(slices, out, version);
    }

    protected long serializedSizeInternal(int version, TypeSizes sizes)
    {
        return Slices.serializer.serializedSize(slices, version, sizes);
    }

    private static class SliceDeserializer extends InternalDeserializer
    {
        public ClusteringIndexFilter deserialize(DataInput in, int version, CFMetaData metadata, boolean reversed) throws IOException
        {
            Slices slices = Slices.serializer.deserialize(in, version, metadata);
            return new ClusteringIndexSliceFilter(slices, reversed);
        }
    }
}
