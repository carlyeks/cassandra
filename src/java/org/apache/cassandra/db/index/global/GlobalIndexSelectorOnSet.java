package org.apache.cassandra.db.index.global;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.composites.CellName;

import java.nio.ByteBuffer;

public class GlobalIndexSelectorOnSet extends GlobalIndexSelector
{
    public GlobalIndexSelectorOnSet(ColumnDefinition columnDefinition)
    {
        super(columnDefinition);
    }

    public boolean canGenerateTombstones()
    {
        return false;
    }

    public boolean selects(CellName cellName)
    {
        return false;
    }

    public ByteBuffer value(CellName cellName, ByteBuffer key, ColumnFamily cf) {
        return null;
    }

}
