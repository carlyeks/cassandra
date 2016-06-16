package org.apache.cassandra.db.compaction;

/**
 * A compaction planner generates all potential compaction tasks from a manifest of files.
 */
public abstract class CompactionPlanner
{
    /**
     * Take a difference in the underlying SSTables and generate a delta to be applied to the compaction plan.
     * This will include all of the CompactionTasks which must be removed, as well as any new CompactionTasks to add.
     *
     * @param previous  This is the previous plan that was used for this strategy.
     * @param manifest  This is the difference of SSTables from the last time we generated a plan.
     * @return          A new CompactionPlan, based on the current one, which includes the necessary changes given the
     *                  changes in the manifest
     */
    public abstract CompactionPlan generate(CompactionPlan previous, CompactionManifest.Difference manifest);
}
