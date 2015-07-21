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

package org.apache.cassandra.db.view;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.MaterializedViewDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.statements.CFProperties;
import org.apache.cassandra.db.AbstractReadCommandBuilder.SinglePartitionSliceBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadOrderGroup;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.partitions.AbstractPartitionData;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A Materialized View copies data from a base table into a view table which can be queried independently from the
 * base. Every update which targets the base table must be fed through the {@link MaterializedViewManager} to ensure
 * that if a view needs to be updated, the updates are properly created and fed into the view.
 */
public class MaterializedView
{
    public final String name;

    public final ColumnFamilyStore viewCfs;
    private final ColumnFamilyStore baseCfs;

    private final AtomicReference<List<ColumnDefinition>> partitionDefs = new AtomicReference<>();
    private final AtomicReference<List<ColumnDefinition>> primaryKeyDefs = new AtomicReference<>();
    private final AtomicReference<List<ColumnDefinition>> baseComplexColumns = new AtomicReference<>();
    private final boolean targetHasAllPrimaryKeyColumns;
    private final boolean includeAll;
    private MaterializedViewBuilder builder;

    public MaterializedView(MaterializedViewDefinition definition,
                            ColumnFamilyStore baseCfs)
    {
        this.baseCfs = baseCfs;

        name = definition.viewName;
        includeAll = definition.includeAll;

        targetHasAllPrimaryKeyColumns = updateDefinition(definition);
        CFMetaData viewCfm = Schema.instance.getCFMetaData(baseCfs.metadata.ksName, definition.viewName);
        viewCfs = Schema.instance.getColumnFamilyStoreInstance(viewCfm.cfId);
    }

    private boolean resolveAndAddColumns(Iterable<ColumnIdentifier> columns, List<ColumnDefinition>... definitions)
    {
        boolean allArePrimaryKeys = true;
        for (ColumnIdentifier identifier: columns)
        {
            ColumnDefinition cdef = baseCfs.metadata.getColumnDefinition(identifier);
            assert cdef != null : "Could not resolve column " + identifier.toString();

            for (List<ColumnDefinition> list: definitions) {
                list.add(cdef);
            }

            allArePrimaryKeys = allArePrimaryKeys && cdef.isPrimaryKeyColumn();
        }
        return allArePrimaryKeys;
    }

    /**
     * This updates the columns stored which are dependent on the base CFMetaData.
     *
     * @return true if the view contains only columns which are part of the base's primary key; false if there is at
     *         least one column which is not.
     */
    public boolean updateDefinition(MaterializedViewDefinition definition)
    {
        List<ColumnDefinition> partitionDefs = new ArrayList<>(definition.partitionColumns.size());
        List<ColumnDefinition> primaryKeyDefs = new ArrayList<>(definition.partitionColumns.size()
                                                                + definition.clusteringColumns.size());
        List<ColumnDefinition> baseComplexColumns = new ArrayList<>();

        // We only add the partition columns to the partitions list, but both partition columns and clustering
        // columns are added to the primary keys list
        boolean partitionAllPrimaryKeyColumns = resolveAndAddColumns(definition.partitionColumns, primaryKeyDefs, partitionDefs);
        boolean clusteringAllPrimaryKeyColumns = resolveAndAddColumns(definition.clusteringColumns, primaryKeyDefs);

        for (ColumnDefinition cdef : baseCfs.metadata.allColumns())
        {
            if (cdef.isComplex())
            {
                baseComplexColumns.add(cdef);
            }
        }

        this.partitionDefs.set(partitionDefs);
        this.primaryKeyDefs.set(primaryKeyDefs);
        this.baseComplexColumns.set(baseComplexColumns);

        return partitionAllPrimaryKeyColumns && clusteringAllPrimaryKeyColumns;
    }

    /**
     * Check to see if the update could possibly modify a view. Cases where the view may be updated are:
     * <ul>
     *     <li>View selects all columns</li>
     *     <li>Update contains any range tombstones</li>
     *     <li>Update touches one of the columns included in the view</li>
     * </ul>
     *
     * If the update contains any range tombstones, there is a possibility that it will not touch a range that is
     * currently included in the view.
     *
     * @return true if {@param upd} modifies a column included in the view
     */
    public boolean updateAffectsView(AbstractPartitionData upd)
    {
        // If we are including all of the columns, then any update will be included
        if (includeAll)
            return true;

        // If there are range tombstones, tombstones will also need to be generated for the materialized view
        // This requires a query of the base rows and generating tombstones for all of those values
        if (!upd.deletionInfo().isLive())
            return true;

        // Check whether the update touches any of the columns included in the view
        for (Row row : upd)
        {
            for (Cell cell : row)
            {
                if (viewCfs.metadata.getColumnDefinition(cell.column().name) != null)
                    return true;
            }
        }

        return false;
    }

    private Object[] viewClustering(TemporalRow temporalRow, TemporalRow.Resolver resolver)
    {
        int numViewClustering = viewCfs.metadata.clusteringColumns().size();
        Object[] viewClusteringValues = new Object[numViewClustering];
        for (int i = 0; i < numViewClustering; i++)
        {
            ColumnDefinition definition = viewCfs.metadata.clusteringColumns().get(i);
            viewClusteringValues[i] = temporalRow.clusteringValue(definition, resolver);
        }

        return viewClusteringValues;
    }

    /**
     * @return Mutation containing a range tombstone for a base partition key and TemporalRow.
     */
    private Mutation createTombstone(TemporalRow temporalRow,
                                     DecoratedKey partitionKey,
                                     long timestamp,
                                     TemporalRow.Resolver resolver)
    {
        return RowUpdateBuilder.deleteRow(viewCfs.metadata, timestamp, partitionKey, viewClustering(temporalRow, resolver));
    }

    /**
     * @return Mutation containing a complex tombstone for a base partition key, a TemporalRow, and the collection's
     *         column identifier.
     */
    private Mutation createComplexTombstone(TemporalRow temporalRow,
                                            DecoratedKey partitionKey,
                                            ColumnDefinition deletedColumn,
                                            long timestamp,
                                            TemporalRow.Resolver resolver)
    {
        return new RowUpdateBuilder(viewCfs.metadata, timestamp, partitionKey)
               .clustering(viewClustering(temporalRow, resolver))
               .resetCollection(deletedColumn)
               .build();
    }

    /**
     * @return View's DecoratedKey or null, if one of the view's primary key components has an invalid resolution from
     *         the TemporalRow and its Resolver
     */
    private DecoratedKey targetPartitionKey(TemporalRow temporalRow, TemporalRow.Resolver resolver)
    {
        List<ColumnDefinition> partitionDefs = this.partitionDefs.get();
        Object[] partitionKey = new Object[partitionDefs.size()];

        for (int i = 0; i < partitionKey.length; i++)
        {
            ByteBuffer value = temporalRow.clusteringValue(partitionDefs.get(i), resolver);

            if (value == null)
                return null;

            partitionKey[i] = value;
        }

        return viewCfs.partitioner.decorateKey(CFMetaData.serializePartitionKey(viewCfs.metadata
                                                                                .getKeyValidatorAsClusteringComparator()
                                                                                .make(partitionKey)));
    }

    /**
     * @return mutation which contains the tombstone for the referenced TemporalRow, or null if not necessary.
     * TemporalRow's can reference at most one view row; there will be at most one row to be tombstoned, so only one
     * mutation is necessary
     */
    private Mutation createPartitionTombstonesForUpdates(TemporalRow temporalRow, long timestamp)
    {
        // Primary Key and Clustering columns do not generate tombstones
        if (targetHasAllPrimaryKeyColumns)
            return null;

        boolean hasUpdate = false;
        List<ColumnDefinition> primaryKeyDefs = this.primaryKeyDefs.get();
        for (ColumnDefinition target : primaryKeyDefs)
        {
            if (!target.isPrimaryKeyColumn() && temporalRow.clusteringValue(target, TemporalRow.oldValueIfUpdated) != null)
                hasUpdate = true;
        }

        if (!hasUpdate)
            return null;

        TemporalRow.Resolver resolver = TemporalRow.earliest;
        return createTombstone(temporalRow, targetPartitionKey(temporalRow, resolver), timestamp, resolver);
    }

    /**
     * @return Mutation which is the transformed base table mutation for the materialized view.
     */
    private Mutation createMutationsForInserts(TemporalRow temporalRow, long timestamp, boolean tombstonesGenerated)
    {
        DecoratedKey partitionKey = targetPartitionKey(temporalRow, TemporalRow.latest);
        if (partitionKey == null)
        {
            // Not having a partition key means we aren't updating anything
            return null;
        }

        TemporalRow.Resolver resolver = tombstonesGenerated
                                         ? TemporalRow.latest
                                         : TemporalRow.newValueIfUpdated;

        RowUpdateBuilder builder = new RowUpdateBuilder(viewCfs.metadata, timestamp, temporalRow.ttl, partitionKey);
        int nowInSec = FBUtilities.nowInSeconds();

        Object[] clustering = new Object[viewCfs.metadata.clusteringColumns().size()];
        for (int i = 0; i < clustering.length; i++)
        {
            clustering[i] = temporalRow.clusteringValue(viewCfs.metadata.clusteringColumns().get(i), resolver);
        }
        builder.clustering(clustering);

        for (ColumnDefinition columnDefinition : viewCfs.metadata.allColumns())
        {
            if (columnDefinition.isPrimaryKeyColumn())
                continue;

            for (Cell cell : temporalRow.values(columnDefinition, resolver, timestamp))
            {
                if (columnDefinition.isComplex())
                {
                    if (cell.isTombstone())
                        builder.addComplex(columnDefinition, cell.path(), ByteBufferUtil.EMPTY_BYTE_BUFFER, cell.livenessInfo());
                    else
                        builder.addComplex(columnDefinition, cell.path(), cell.isLive(nowInSec) ? cell.value() : null, cell.livenessInfo());
                }
                else
                {
                    builder.add(columnDefinition, cell.isLive(nowInSec) ? cell.value() : null, cell.livenessInfo());
                }
            }
        }

        return builder.build();
    }

    /**
     * @param upd Update which possibly contains deletion info for which to generate view tombstones.
     * @return    View Tombstones which delete all of the rows which have been removed from the base table with
     *            {@param upd}
     */
    private Collection<Mutation> createForDeletionInfo(TemporalRow.Set rowSet, AbstractPartitionData upd)
    {
        final TemporalRow.Resolver resolver = TemporalRow.earliest;

        DeletionInfo deletionInfo = upd.deletionInfo();

        List<Mutation> mutations = new ArrayList<>();

        // Check the complex columns to see if there are any which may have tombstones we need to create for the view
        if (!baseComplexColumns.get().isEmpty())
        {
            for (Row row : upd)
            {
                if (!row.hasComplexDeletion())
                    continue;

                TemporalRow temporalRow = rowSet.getExistingUnit(row);

                assert temporalRow != null;

                for (ColumnDefinition definition : baseComplexColumns.get())
                {
                    DeletionTime time = row.getDeletion(definition);
                    if (!time.isLive())
                    {
                        DecoratedKey targetKey = targetPartitionKey(temporalRow, resolver);
                        if (targetKey != null)
                            mutations.add(createComplexTombstone(temporalRow, targetKey, definition, upd.maxTimestamp(), resolver));
                    }
                }
            }
        }

        if (!deletionInfo.isLive())
        {
            // We have to generate tombstones for all of the affected rows, but we don't have the information in order
            // to create them. This requires that we perform a read for the entire range that is being tombstoned, and
            // generate a tombstone for each. This may be slow, because a single range tombstone can cover up to an
            // entire partition of data which is not distributed on a single partition node.
            ReadCommand command;
            DecoratedKey dk = rowSet.dk;

            long timestamp;
            if (deletionInfo.hasRanges())
            {
                SinglePartitionSliceBuilder builder = new SinglePartitionSliceBuilder(baseCfs, dk);
                Iterator<RangeTombstone> tombstones = deletionInfo.rangeIterator(false);
                timestamp = Long.MIN_VALUE;
                while (tombstones.hasNext())
                {
                    RangeTombstone tombstone = tombstones.next();

                    builder.addSlice(tombstone.deletedSlice());
                    timestamp = Math.max(timestamp, tombstone.deletionTime().markedForDeleteAt());
                }

                if (!mutations.isEmpty())
                    return mutations;

                command = builder.build();
            }
            else
            {
                timestamp = deletionInfo.getPartitionDeletion().markedForDeleteAt();
                command = SinglePartitionReadCommand.fullPartitionRead(baseCfs.metadata, FBUtilities.nowInSeconds(), dk);
            }

            QueryPager pager = command.getPager(null);

            // Add all of the rows which were recovered from the query to the row set
            while (!pager.isExhausted())
            {
                try (ReadOrderGroup orderGroup = pager.startOrderGroup();
                     PartitionIterator iter = pager.fetchPageInternal(128, orderGroup))
                {
                    if (!iter.hasNext())
                        break;

                    try (RowIterator rowIterator = iter.next())
                    {
                        while (rowIterator.hasNext())
                        {
                            Row row = rowIterator.next();
                            rowSet.addRow(row, false);
                        }
                    }
                }
            }

            // If the temporal row has been deleted by the deletion info, we generate the corresponding range tombstone
            // for the view.
            for (TemporalRow temporalRow : rowSet)
            {
                if (!temporalRow.isLive(deletionInfo, resolver))
                {
                    DecoratedKey value = targetPartitionKey(temporalRow, resolver);
                    if (value != null)
                    {
                        Mutation mutation = createTombstone(temporalRow, value, timestamp, resolver);
                        if (mutation != null)
                            mutations.add(mutation);
                    }
                }
            }
        }

        return !mutations.isEmpty() ? mutations : null;
    }

    /**
     * Read and update temporal rows in the set which have corresponding values stored on the local node
     */
    private void readLocalRows(TemporalRow.Set rowSet)
    {
        SinglePartitionSliceBuilder builder = new SinglePartitionSliceBuilder(baseCfs, rowSet.dk);

        for (TemporalRow temporalRow : rowSet)
            builder.addSlice(temporalRow.baseSlice());

        QueryPager pager = builder.build().getPager(null);

        while (!pager.isExhausted())
        {
            try (ReadOrderGroup orderGroup = pager.startOrderGroup();
                 PartitionIterator iter = pager.fetchPageInternal(128, orderGroup))
            {
                while (iter.hasNext())
                {
                    try (RowIterator rows = iter.next())
                    {
                        while (rows.hasNext())
                        {
                            rowSet.addRow(rows.next(), false);
                        }
                    }
                }
            }
        }
    }

    /**
     * @return Set of rows which are contained in the partition update {@param upd}
     */
    private TemporalRow.Set separateRows(ByteBuffer key, AbstractPartitionData upd)
    {
        TemporalRow.Set rowSet = new TemporalRow.Set(baseCfs, key);

        for (Row row : upd)
            rowSet.addRow(row, true);

        return rowSet;
    }

    /**
     * @param isBuilding If the view is currently being built, we do not query the values which are already stored,
     *                   since all of the update will already be present in the base table.
     * @return View mutations which represent the changes necessary as long as previously created mutations for the view
     *         have been applied successfully. This is based solely on the changes that are necessary given the current
     *         state of the base table and the newly applying partition data.
     */
    public Collection<Mutation> createMutations(ByteBuffer key, AbstractPartitionData upd, boolean isBuilding)
    {
        if (!updateAffectsView(upd))
            return null;

        TemporalRow.Set rowSet = separateRows(key, upd);

        // If we are building the view, we do not want to add old values; they will always be the same
        if (!isBuilding)
            readLocalRows(rowSet);

        Collection<Mutation> mutations = null;
        for (TemporalRow temporalRow : rowSet)
        {
            boolean tombstonesInserted = false;

            // If we are building, there is no need to check for partition tombstones; those values will not be present
            // in the partition data
            if (!isBuilding)
            {
                Mutation partitionTombstone = createPartitionTombstonesForUpdates(temporalRow, upd.maxTimestamp());
                tombstonesInserted = partitionTombstone != null;
                if (tombstonesInserted)
                {
                    if (mutations == null) mutations = new LinkedList<>();
                    mutations.add(partitionTombstone);
                }
            }

            Mutation insert = createMutationsForInserts(temporalRow, upd.maxTimestamp(), tombstonesInserted);
            if (insert != null)
            {
                if (mutations == null) mutations = new LinkedList<>();
                mutations.add(insert);
            }
        }

        if (!isBuilding)
        {
            Collection<Mutation> deletion = createForDeletionInfo(rowSet, upd);
            if (deletion != null && !deletion.isEmpty())
            {
                if (mutations == null) mutations = new LinkedList<>();
                mutations.addAll(deletion);
            }
        }

        return mutations;
    }

    public synchronized void build()
    {
        if (this.builder != null)
        {
            this.builder.stop();
            this.builder = null;
        }

        this.builder = new MaterializedViewBuilder(baseCfs, this);
        CompactionManager.instance.submitMaterializedViewBuilder(builder);
    }

    /**
     * @return CFMetaData which represents the definition given
     */
    public static CFMetaData getCFMetaData(MaterializedViewDefinition definition,
                                           CFMetaData baseCf,
                                           CFProperties properties)
    {
        CFMetaData.Builder viewBuilder = CFMetaData.Builder
                                         .create(baseCf.ksName, definition.viewName);

        ColumnDefinition nonPkTarget = null;

        for (ColumnIdentifier targetIdentifier : definition.partitionColumns)
        {
            ColumnDefinition target = baseCf.getColumnDefinition(targetIdentifier);
            if (!target.isPartitionKey())
                nonPkTarget = target;

            viewBuilder.addPartitionKey(target.name, properties.getReversableType(targetIdentifier, target.type));
        }

        Collection<ColumnDefinition> included = new ArrayList<>();
        for(ColumnIdentifier identifier : definition.included)
        {
            ColumnDefinition cfDef = baseCf.getColumnDefinition(identifier);
            assert cfDef != null;
            included.add(cfDef);
        }

        boolean includeAll = included.isEmpty();

        for (ColumnIdentifier ident : definition.clusteringColumns)
        {
            ColumnDefinition column = baseCf.getColumnDefinition(ident);
            viewBuilder.addClusteringColumn(ident, properties.getReversableType(ident, column.type));
        }

        for (ColumnDefinition column : baseCf.partitionColumns().regulars.columns)
        {
            if (column != nonPkTarget && (includeAll || included.contains(column)))
            {
                viewBuilder.addRegularColumn(column.name, column.type);
            }
        }

        //Add any extra clustering columns
        for (ColumnDefinition column : Iterables.concat(baseCf.partitionKeyColumns(), baseCf.clusteringColumns()))
        {
            if ( (!definition.partitionColumns.contains(column.name) && !definition.clusteringColumns.contains(column.name)) &&
                 (includeAll || included.contains(column)) )
            {
                viewBuilder.addRegularColumn(column.name, column.type);
            }
        }

        CFMetaData cfm = viewBuilder.build();
        properties.properties.applyToCFMetadata(cfm);

        return cfm;
    }
}
