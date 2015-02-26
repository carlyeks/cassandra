package org.apache.cassandra.db.index.global;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.CFRowAdder;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.composites.CBuilder;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.CollectionType;

public abstract class GlobalIndexSelector
{
    public final ColumnDefinition columnDefinition;
    protected GlobalIndexSelector(ColumnDefinition columnDefinition)
    {
        this.columnDefinition = columnDefinition;
    }

    public static GlobalIndexSelector create(ColumnFamilyStore baseCfs, ColumnDefinition cfDef)
    {
        if (cfDef.type.isCollection() && cfDef.type.isMultiCell())
        {
            switch (((CollectionType)cfDef.type).kind)
            {
                case LIST:
                    return new GlobalIndexSelectorOnList(cfDef);
                case SET:
                    return new GlobalIndexSelectorOnSet(cfDef);
                case MAP:
                    return new GlobalIndexSelectorOnMap(cfDef);
            }
        }

        switch (cfDef.kind)
        {
            case CLUSTERING_COLUMN:
                return new GlobalIndexSelectorOnClusteringColumn(cfDef);
            case REGULAR:
                return new GlobalIndexSelectorOnRegularColumn(baseCfs, cfDef);
            case PARTITION_KEY:
                return new GlobalIndexSelectorOnPartitionKey(baseCfs, cfDef);
        }
        throw new AssertionError();
    }

    /**
     * Depending on whether this column can overwrite the values of a different
     * @return True if a check for tombstones needs to be done, false otherwise
     */
    public abstract boolean canGenerateTombstones();

    public boolean isPrimaryKey()
    {
        return false;
    }

    public abstract boolean selects(CellName cellName);

    public abstract ByteBuffer value(CellName cellName, ByteBuffer key, ColumnFamily cf);

    public ByteBuffer value(ByteBuffer key)
    {
        throw new AssertionError("Cannot create a value from partition key");
    }
}
