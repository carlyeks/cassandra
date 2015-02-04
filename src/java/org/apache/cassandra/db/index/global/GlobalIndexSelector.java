package org.apache.cassandra.db.index.global;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.marshal.CollectionType;

public abstract class  GlobalIndexSelector
{
    public static GlobalIndexSelector create(ColumnDefinition cfDef)
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
                return new GlobalIndexSelectorOnRegularColumn(cfDef);
            case PARTITION_KEY:
                return new GlobalIndexSelectorOnPartitionKey(cfDef);
        }
        throw new AssertionError();
    }

    public abstract boolean selects(CellName cellName);

    public abstract CellName cellName(CellName cellName);
}
