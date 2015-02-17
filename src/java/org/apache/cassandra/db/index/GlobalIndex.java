package org.apache.cassandra.db.index;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.CompoundSparseCellNameType;
import org.apache.cassandra.db.index.global.GlobalIndexSelector;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.ByteBufferUtil;

public class GlobalIndex
{
    private ColumnDefinition target;
    private Collection<ColumnDefinition> denormalized;

    private GlobalIndexSelector targetSelector;
    private List<GlobalIndexSelector> clusteringSelectors;
    private List<GlobalIndexSelector> regularSelectors;
    private List<GlobalIndexSelector> staticSelectors;

    private ColumnFamilyStore baseCfs;
    private ColumnFamilyStore indexCfs;

    public GlobalIndex(ColumnDefinition target, Collection<ColumnDefinition> denormalized, ColumnFamilyStore baseCfs)
    {
        this.target = target;
        this.denormalized = denormalized;
        this.baseCfs = baseCfs;

        clusteringSelectors = new ArrayList<>();
        regularSelectors = new ArrayList<>();
        staticSelectors = new ArrayList<>();

        createIndexCfsAndSelectors();
    }

    private void createIndexCfsAndSelectors()
    {
        assert baseCfs != null;
        assert target != null;

        CFMetaData indexedCfMetadata = getCFMetaData(baseCfs.metadata, target, denormalized);
        targetSelector = GlobalIndexSelector.create(baseCfs, target);

        // All partition and clustering columns are included in the index, whether they are specified in the denormalized columns or not
        for (ColumnDefinition column: baseCfs.metadata.partitionKeyColumns())
        {
            if (column != target)
            {
                clusteringSelectors.add(GlobalIndexSelector.create(baseCfs, column));
            }
        }

        for (ColumnDefinition column: baseCfs.metadata.clusteringColumns())
        {
            if (column != target)
            {
                clusteringSelectors.add(GlobalIndexSelector.create(baseCfs, column));
            }
        }

        for (ColumnDefinition column: baseCfs.metadata.regularColumns())
        {
            if (column != target && denormalized.contains(column))
            {
                regularSelectors.add(GlobalIndexSelector.create(baseCfs, column));
            }
        }

        for (ColumnDefinition column: baseCfs.metadata.staticColumns())
        {
            if (column != target && denormalized.contains(column))
            {
                staticSelectors.add(GlobalIndexSelector.create(baseCfs, column));
            }
        }

        indexCfs = Schema.instance.getColumnFamilyStoreInstance(indexedCfMetadata.cfId);
    }

    /**
     * Check to see if any value that is part of the index is updated. If so, we possibly need to mutate the index.
     *
     * @param cf Column family to check for indexed values with
     * @return True if any of the indexed or denormalized values are contained in the column family.
     */
    private boolean modifiesIndexedColumn(ColumnFamily cf)
    {
        // If we are denormalizing all of the columns, then any non-empty column family will need to be indexed
        if (denormalized.isEmpty() && !cf.isEmpty())
            return true;

        for (CellName cellName : cf.getColumnNames())
        {
            if (targetSelector.selects(cellName))
                return true;
            for (GlobalIndexSelector column: Iterables.concat(clusteringSelectors, regularSelectors, staticSelectors))
            {
                if (column.selects(cellName))
                    return true;
            }
        }
        return false;
    }

    private boolean modifiesTarget(ColumnFamily cf)
    {
        for (CellName cellName: cf.getColumnNames())
        {
            if (targetSelector.selects(cellName))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * @param cf Column family being modified
     * @param previousResults Current values that are stored for the specified partition key
     */
    private Collection<Mutation> createTombstones(ByteBuffer key, ColumnFamily cf, List<Row> previousResults)
    {
        if (!targetSelector.canGenerateTombstones())
            return Collections.emptyList();

        if (previousResults.isEmpty())
            return Collections.emptyList();

        if (!modifiesTarget(cf))
            return Collections.emptyList();

        List<Mutation> tombstones = new ArrayList<>();
        for (Row row: previousResults)
        {
            ColumnFamily rowCf = row.cf;
            if (rowCf == null) continue;

            GlobalIndexSelector.Holder holder = new GlobalIndexSelector.Holder(targetSelector, clusteringSelectors, regularSelectors, staticSelectors);

            holder.updatePartitionKey(key);

            for (CellName cellName: rowCf.getColumnNames())
            {
                holder.update(cellName, key, rowCf);
            }

            Mutation mutation = holder.getTombstoneMutation(indexCfs, rowCf.maxTimestamp());
            if (mutation != null)
                tombstones.add(mutation);
        }
        return tombstones;
    }

    private Collection<Mutation> createInserts(ByteBuffer key, ColumnFamily cf, List<Row> results)
    {
        GlobalIndexSelector.Holder holder = new GlobalIndexSelector.Holder(targetSelector, clusteringSelectors, regularSelectors, staticSelectors);

        // It is possible the that the index partition key is not known, so we have to look at the previous results for it
        holder.updatePartitionKey(key);
        for (CellName cellName: cf.getColumnNames())
        {
            holder.update(cellName, key, cf);
        }
        Mutation mutation = holder.getMutation(indexCfs, cf.maxTimestamp());
        if (mutation != null)
            return Collections.singleton(mutation);
        return Collections.emptyList();
    }

    public Collection<Mutation> createMutations(ByteBuffer key, ColumnFamily cf, ConsistencyLevel consistency)
    {
        if (!modifiesIndexedColumn(cf))
        {
            return null;
        }

        // Need to execute a read first (this is *not* a local read; it should be done at the same consistency as write
        List<Row> results = StorageProxy.read(Collections.<ReadCommand>singletonList(new SliceFromReadCommand(cf.metadata().ksName,
                                                                                                              key,
                                                                                                              cf.metadata().cfName,
                                                                                                              cf.maxTimestamp(),
                                                                                                              new IdentityQueryFilter())), consistency);

        Collection<Mutation> mutations = null;
        Collection<Mutation> tombstones = createTombstones(key, cf, results);
        if (tombstones != null && !tombstones.isEmpty())
        {
            if (mutations == null) mutations = new ArrayList<>();
            mutations.addAll(tombstones);
        }

        Collection<Mutation> inserts = createInserts(key, cf, results);

        if (inserts != null && !inserts.isEmpty())
        {
            if (mutations == null) mutations = new ArrayList<>();
            mutations.addAll(inserts);
        }

        return mutations;
    }

    public static CFMetaData getCFMetaData(CFMetaData baseCFMD, ColumnDefinition target, Collection<ColumnDefinition> denormalized)
    {
        String name = baseCFMD.cfName + "_" + ByteBufferUtil.bytesToHex(target.name.bytes);
        UUID cfId = Schema.instance.getId(baseCFMD.ksName, name);
        if (cfId != null)
            return Schema.instance.getCFMetaData(cfId);

        CFMetaData indexedCfMetadata = CFMetaData.createGlobalIndexMetadata(name, baseCFMD, target, getIndexComparator(baseCFMD, target));

        indexedCfMetadata.addColumnDefinition(ColumnDefinition.partitionKeyDef(indexedCfMetadata, target.name.bytes, target.type, null));

        // All partition and clustering columns are included in the index, whether they are specified in the denormalized columns or not
        for (ColumnDefinition column: baseCFMD.partitionKeyColumns())
        {
            if (column != target)
            {
                Integer position = null;
                if (!column.isOnAllComponents())
                    position = column.position();
                indexedCfMetadata.addColumnDefinition(ColumnDefinition.clusteringKeyDef(indexedCfMetadata, column.name.bytes, column.type, position));
            }
        }

        for (ColumnDefinition column: baseCFMD.clusteringColumns())
        {
            if (column != target)
            {
                Integer position = null;
                if (!column.isOnAllComponents())
                    position = column.position();
                indexedCfMetadata.addColumnDefinition(ColumnDefinition.clusteringKeyDef(indexedCfMetadata, column.name.bytes, column.type, position));
            }
        }

        for (ColumnDefinition column: baseCFMD.regularColumns())
        {
            if (column != target && denormalized.contains(column))
            {
                Integer position = null;
                if (!column.isOnAllComponents())
                    position = column.position();
                indexedCfMetadata.addColumnDefinition(ColumnDefinition.regularDef(indexedCfMetadata, column.name.bytes, column.type, position));
            }
        }

        for (ColumnDefinition column: baseCFMD.staticColumns())
        {
            if (column != target && denormalized.contains(column))
            {
                Integer position = null;
                if (!column.isOnAllComponents())
                    position = column.position();
                indexedCfMetadata.addColumnDefinition(ColumnDefinition.staticDef(indexedCfMetadata, column.name.bytes, column.type, position));
            }
        }

        return indexedCfMetadata;
    }

    public static CellNameType getIndexComparator(CFMetaData baseCFMD, ColumnDefinition target)
    {
        List<AbstractType<?>> types = new ArrayList<>();
        // All partition and clustering columns are included in the index, whether they are specified in the denormalized columns or not
        for (ColumnDefinition column: baseCFMD.partitionKeyColumns())
        {
            if (column != target)
            {
                types.add(column.type);
            }
        }

        for (ColumnDefinition column: baseCFMD.clusteringColumns())
        {
            if (column != target)
            {
                types.add(column.type);
            }
        }
        return new CompoundSparseCellNameType(types);
    }
}