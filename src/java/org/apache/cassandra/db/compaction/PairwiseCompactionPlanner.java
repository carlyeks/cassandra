package org.apache.cassandra.db.compaction;

import org.apache.cassandra.io.sstable.format.SSTableReader;

public class PairwiseCompactionPlanner extends CompactionPlanner
{
    public CompactionPlan generate(CompactionPlan previous, CompactionManifest.Difference manifest)
    {
        CompactionPlan.Difference difference = new CompactionPlan.Difference();
        if (manifest.hasDeletions())
        {
            for (SSTableReader reader : manifest.deletions())
            {
                for (CompactionTask task : previous.including(reader))
                {
                    difference.remove(task);
                }
            }
        }
        if (manifest.hasAdditions())
        {
            for (SSTableReader reader : manifest.additions())
            {
                for (SSTableReader other : previous.manifest())
                {
                    difference.add(reader, other);
                }
            }
        }
        return difference.applyTo(previous);
    }
}
