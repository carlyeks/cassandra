package org.apache.cassandra.db.compaction;

import org.apache.cassandra.io.sstable.format.SSTableReader;

public class CompactionPlan
{
    public CompactionManifest manifest()
    {
        return null;
    }

    public Iterable<CompactionTask> including(SSTableReader reader)
    {
        return null;
    }

    public static class Difference {

        public void add(SSTableReader... readers)
        {

        }

        public void remove(CompactionTask task)
        {

        }

        public CompactionPlan applyTo(CompactionPlan previous)
        {
            return null;
        }
    }
}
