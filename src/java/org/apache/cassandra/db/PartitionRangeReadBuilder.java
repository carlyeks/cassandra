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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.NamesPartitionFilter;
import org.apache.cassandra.db.filter.PartitionFilter;
import org.apache.cassandra.db.filter.SlicePartitionFilter;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.utils.FBUtilities;

public class PartitionRangeReadBuilder extends AbstractReadCommandBuilder
{
    public enum RangeType
    {
        Inclusive,
        Exclusive,
        Range,
        ReverseRange
    }

    private RangeType rangeType = RangeType.Inclusive;
    private ByteBuffer startKey;
    private ByteBuffer endKey;
    private SortedSet<Clustering> clusterings;

    public PartitionRangeReadBuilder(ColumnFamilyStore cfs)
    {
        this(cfs, FBUtilities.nowInSeconds());
    }

    public PartitionRangeReadBuilder(ColumnFamilyStore cfs, int nowInSeconds)
    {
        super(cfs, nowInSeconds);

        clusterings = new TreeSet<>(cfs.getComparator());
    }

    public PartitionRangeReadBuilder setKeyBounds(ByteBuffer start, ByteBuffer end)
    {
        startKey = start;
        endKey = end;
        return this;
    }

    public PartitionRangeReadBuilder setRangeType(RangeType type)
    {
        rangeType = type;
        return this;
    }

    public PartitionRangeReadBuilder addClustering(Object... objects)
    {
        CBuilder builder = CBuilder.create(cfs.getComparator());
        for (Object j : objects)
        {
            builder.add(j);
        }
        clusterings.add(builder.build());
        return this;
    }

    @Override
    public ReadCommand build()
    {
        DataRange dr;
        if (startKey == null && endKey == null && columns.size() == 0)
        {
            dr = DataRange.allData(cfs.metadata, (cfs.partitioner));
        }
        else
        {
            PartitionColumns.Builder builder = new PartitionColumns.Builder();
            if (columns.size() == 0)
            {
                // Add all non-clustering, non-pk columns if none are specified
                for (ColumnDefinition cd : cfs.metadata.allColumns())
                {
                    if (cd.isRegular())
                        builder.add(cd);
                }
            }
            else
            {
                for (ByteBuffer colName : columns)
                {
                    ColumnDefinition cd = cfs.metadata.getColumnDefinition(colName);
                    assert cd.kind == ColumnDefinition.Kind.REGULAR || cd.kind == ColumnDefinition.Kind.STATIC;
                    builder.add(cfs.metadata.getColumnDefinition(colName));
                }
            }

            Slices slices = lowerClusteringBound == null || upperClusteringBound == null
                    ? Slices.ALL
                    : Slices.with(cfs.getComparator(), Slice.make(lowerClusteringBound, upperClusteringBound));

            PartitionFilter filter = clusterings.isEmpty()
                    ? new SlicePartitionFilter(builder.build(), slices, reversed)
                    : new NamesPartitionFilter(builder.build(), clusterings, reversed);

            if (startKey == null || endKey == null)
            {
                dr = DataRange.allData(cfs.partitioner, filter);
            }
            else
            {
                AbstractBounds<PartitionPosition> bounds = null;
                DecoratedKey s = cfs.partitioner.decorateKey(startKey);
                DecoratedKey e = cfs.partitioner.decorateKey(endKey);
                switch (rangeType)
                {
                    case Inclusive:
                        bounds = new Bounds<>(s, e);
                        break;
                    case Exclusive:
                        bounds = new ExcludingBounds<>(s, e);
                        break;
                    case Range:
                        bounds = new Range<>(s, e);
                        break;
                    case ReverseRange:
                        bounds = new IncludingExcludingBounds<>(s, e);
                        break;
                }
                dr = new DataRange(bounds, filter);
            }
        }

        if (superColumn != null)
            filter.add(cfs.metadata.compactValueColumn(), Operator.EQ, superColumn);

        DataLimits limits = DataLimits.cqlLimits(cqlLimit);
        if (pagingLimit != -1)
            limits = limits.forPaging(pagingLimit);

        return new PartitionRangeReadCommand(cfs.metadata,
                                             nowInSeconds,
                                             filter,
                                             limits,
                                             dr);
    }
}
