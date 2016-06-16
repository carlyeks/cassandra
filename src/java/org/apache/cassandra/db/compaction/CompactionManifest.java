package org.apache.cassandra.db.compaction;

import org.apache.cassandra.io.sstable.format.SSTableReader;

public class CompactionManifest
{
    public static class Difference {


        public Iterable<? extends SSTableReader> additions()
        {
            return null;
        }

        public boolean hasAdditions()
        {
            return false;
        }

        public Iterable<? extends SSTableReader> deletions()
        {
            return null;
        }

        public boolean hasDeletions()
        {
            return false;
        }
    }
}
