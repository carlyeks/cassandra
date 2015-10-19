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
package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;

public class LeveledManifest implements CompactionManifest
{
    private static final Logger logger = LoggerFactory.getLogger(LeveledManifest.class);

    /**
     * limit the number of L0 sstables we do at once, because compaction bloom filter creation
     * uses a pessimistic estimate of how many keys overlap (none), so we risk wasting memory
     * or even OOMing when compacting highly overlapping sstables
     */
    private static final int MAX_COMPACTING_OVERLAPPING = 32;
    /**
     * If we go this many rounds without compacting
     * in the highest level, we start bringing in sstables from
     * that level into lower level compactions
     */
    private static final int NO_COMPACTION_LIMIT = 25;
    // allocate enough generations for a PB of data, with a 1-MB sstable size.  (Note that if maxSSTableSize is
    // updated, we will still have sstables of the older, potentially smaller size.  So don't make this
    // dependent on maxSSTableSize.)
    public static final int MAX_LEVEL_COUNT = (int) Math.log10(1000 * 1000 * 1000);
    private final ColumnFamilyStore cfs;
    private final String tableId;
    @VisibleForTesting
    protected final List<SSTableReader>[] generations;
    private final PartitionPosition[] lastCompactedKeys;
    private final long maxSSTableSizeInBytes;
    private final int maxOverlappingLevel;
    private final SizeTieredCompactionStrategyOptions options;
    private final int [] compactionCounter;

    LeveledManifest(ColumnFamilyStore cfs, int maxSSTableSizeInMB, int maxOverlappingLevel, SizeTieredCompactionStrategyOptions options)
    {
        this.cfs = cfs;
        this.tableId = cfs.metadata.ksName + "." + cfs.metadata.cfName;
        this.maxSSTableSizeInBytes = maxSSTableSizeInMB * 1024L * 1024L;
        this.maxOverlappingLevel = maxOverlappingLevel;
        this.options = options;

        generations = new List[MAX_LEVEL_COUNT];
        lastCompactedKeys = new PartitionPosition[MAX_LEVEL_COUNT];
        for (int i = 0; i < generations.length; i++)
        {
            generations[i] = new ArrayList<>();
            lastCompactedKeys[i] = cfs.getPartitioner().getMinimumToken().minKeyBound();
        }
        compactionCounter = new int[MAX_LEVEL_COUNT];
    }

    public static LeveledManifest create(ColumnFamilyStore cfs, int maxSSTableSize, int maxOverlappingLevel, List<SSTableReader> sstables)
    {
        return create(cfs, maxSSTableSize, maxOverlappingLevel, sstables, new SizeTieredCompactionStrategyOptions());
    }

    public static LeveledManifest create(ColumnFamilyStore cfs, int maxSSTableSize, int maxOverlappingLevel, Iterable<SSTableReader> sstables, SizeTieredCompactionStrategyOptions options)
    {
        LeveledManifest manifest = new LeveledManifest(cfs, maxSSTableSize, maxOverlappingLevel, options);

        // ensure all SSTables are in the manifest
        for (SSTableReader ssTableReader : sstables)
        {
            manifest.add(ssTableReader);
        }
        // Start at the first non-overlapping level
        for (int i = maxOverlappingLevel + 1; i < manifest.getAllLevelSize().length; i++)
        {
            manifest.repairOverlappingSSTables(i);
        }
        return manifest;
    }

    public synchronized void add(SSTableReader reader)
    {
        int level = reader.getSSTableLevel();

        assert level < generations.length : "Invalid level " + level + " out of " + (generations.length - 1);
        logDistribution();
        if (canAddSSTable(reader))
        {
            // adding the sstable does not cause overlap in the level
            logger.trace("Adding {} to {} L{}", reader, tableId, level);
            generations[level].add(reader);
        }
        else
        {
            // this can happen if:
            // * a compaction has promoted an overlapping sstable to the given level, or
            //   was also supposed to add an sstable at the given level.
            // * we are moving sstables from unrepaired to repaired and the sstable
            //   would cause overlap
            //
            // The add(..):ed sstable will be sent to level 0
            try
            {
                reader.descriptor.getMetadataSerializer().mutateLevel(reader.descriptor, 0);
                reader.reloadSSTableMetadata();
            }
            catch (IOException e)
            {
                logger.error("Could not change sstable level - adding it at level 0 anyway, we will find it at restart.", e);
            }
            generations[0].add(reader);
        }
    }

    public synchronized void replace(Collection<SSTableReader> removed, Collection<SSTableReader> added)
    {
        assert !removed.isEmpty(); // use add() instead of promote when adding new sstables
        logDistribution();
        if (logger.isTraceEnabled())
            logger.trace("{} Replacing [{}]", tableId, toString(removed));

        // the level for the added sstables is the max of the removed ones,
        // plus one if the removed were all on the same level
        int minLevel = Integer.MAX_VALUE;

        for (SSTableReader sstable : removed)
        {
            int thisLevel = remove(sstable);
            minLevel = Math.min(minLevel, thisLevel);
        }

        // it's valid to do a remove w/o an add (e.g. on truncate)
        if (added.isEmpty())
            return;

        if (logger.isTraceEnabled())
            logger.trace("{} Adding [{}]", tableId, toString(added));

        for (SSTableReader ssTableReader : added)
            add(ssTableReader);
        lastCompactedKeys[minLevel] = SSTableReader.sstableOrdering.max(added).last;
    }

    public synchronized void repairOverlappingSSTables(int level)
    {
        SSTableReader previous = null;
        Collections.sort(generations[level], SSTableReader.sstableComparator);
        List<SSTableReader> outOfOrderSSTables = new ArrayList<>();
        for (SSTableReader current : generations[level])
        {
            if (previous != null && current.first.compareTo(previous.last) <= 0)
            {
                logger.warn(String.format("At level %d, %s [%s, %s] overlaps %s [%s, %s].  This could be caused by a bug in Cassandra 1.1.0 .. 1.1.3 or due to the fact that you have dropped sstables from another node into the data directory. " +
                                          "Sending back to L0.  If you didn't drop in sstables, and have not yet run scrub, you should do so since you may also have rows out-of-order within an sstable",
                                          level, previous, previous.first, previous.last, current, current.first, current.last));
                outOfOrderSSTables.add(current);
            }
            else
            {
                previous = current;
            }
        }

        if (!outOfOrderSSTables.isEmpty())
        {
            for (SSTableReader sstable : outOfOrderSSTables)
                sendBackToL0(sstable);
        }
    }

    /**
     * Checks if adding the sstable creates an overlap in the level
     * @param sstable the sstable to add
     * @return true if it is safe to add the sstable in the level.
     */
    private boolean canAddSSTable(SSTableReader sstable)
    {
        int level = sstable.getSSTableLevel();
        if (level <= maxOverlappingLevel)
            return true;

        List<SSTableReader> copyLevel = new ArrayList<>(generations[level]);
        copyLevel.add(sstable);
        Collections.sort(copyLevel, SSTableReader.sstableComparator);

        SSTableReader previous = null;
        for (SSTableReader current : copyLevel)
        {
            if (previous != null && current.first.compareTo(previous.last) <= 0)
                return false;
            previous = current;
        }
        return true;
    }

    private synchronized void sendBackToL0(SSTableReader sstable)
    {
        remove(sstable);
        try
        {
            sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 0);
            sstable.reloadSSTableMetadata();
            add(sstable);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Could not reload sstable meta data", e);
        }
    }

    private String toString(Collection<SSTableReader> sstables)
    {
        StringBuilder builder = new StringBuilder();
        for (SSTableReader sstable : sstables)
        {
            builder.append(sstable.descriptor.cfname)
                   .append('-')
                   .append(sstable.descriptor.generation)
                   .append("(L")
                   .append(sstable.getSSTableLevel())
                   .append("), ");
        }
        return builder.toString();
    }

    public static long maxBytesForLevel(int level, long maxSSTableSizeInBytes)
    {
        if (level == 0)
            return 4L * maxSSTableSizeInBytes;
        double bytes = Math.pow(10, level) * maxSSTableSizeInBytes;
        if (bytes > Long.MAX_VALUE)
            throw new RuntimeException("At most " + Long.MAX_VALUE + " bytes may be in a compaction level; your maxSSTableSize must be absurdly high to compute " + bytes);
        return (long) bytes;
    }

    private CompactionCandidate getCompactionCandidateForLevel(int level)
    {
        List<SSTableReader> sstables = getLevel(level);
        if (sstables.isEmpty())
            return null;

        // we want to calculate score excluding compacting ones
        Set<SSTableReader> sstablesInLevel = Sets.newHashSet(sstables);
        Set<SSTableReader> remaining = Sets.difference(sstablesInLevel, cfs.getTracker().getCompacting());
        double score = (double) SSTableReader.getTotalBytes(remaining) / (double) maxBytesForLevel(level, maxSSTableSizeInBytes);
        logger.debug("Compaction score for {} level {} is {}", tableId, level, score);

        if (score > 1.001)
        {
            Collection<SSTableReader> candidates = getCandidatesForUplevelCompaction(level);
            if (!candidates.isEmpty())
            {
                int nextLevel = getNextLevel(candidates);
                candidates = getOverlappingStarvedSSTables(nextLevel, candidates);
                if (logger.isDebugEnabled())
                    logger.debug("Compaction candidates for {} L{} are {}", tableId, level, toString(candidates));
                return new CompactionCandidate(candidates, nextLevel, cfs.getCompactionStrategyManager().getMaxSSTableBytes());
            }
            else
            {
                logger.debug("No compaction candidates for {} L{}", tableId, level);
            }
        }

        return null;
    }

    /**
     * @return highest-priority sstables to compact, and level to compact them to
     * If no compactions are necessary, will return null
     */
    public synchronized CompactionCandidate getCompactionCandidates()
    {
        // during bootstrap we only do size tiering in L0 to make sure
        // the streamed files can be placed in their original levels
        if (StorageService.instance.isBootstrapMode())
        {
            List<SSTableReader> mostInteresting = getSSTablesForSTCS(getLevel(0));
            if (!mostInteresting.isEmpty())
            {
                logger.info("Bootstrapping - doing STCS in {} L0", cfs.metadata.ksName, cfs.metadata.cfName);
                return new CompactionCandidate(mostInteresting, 0, Long.MAX_VALUE);
            }
            return null;
        }

        for (int i = 0; i < maxOverlappingLevel; i++)
        {
            CompactionCandidate candidate = getCompactionCandidateForLevel(i);
            if (candidate != null)
                return candidate;
        }

        for (int i = generations.length - 1; i >= maxOverlappingLevel; i--)
        {
            CompactionCandidate candidate = getCompactionCandidateForLevel(i);
            if (candidate != null)
                return candidate;
        }

        // All levels are less than the maximum, so let's see if we should do a consolidating compaction in any
        // overlapping level
        int overlappingLevel = -1;
        double overlappingScore = 0.0;
        int sstableCountLevel = -1;
        double sstableCountScore = 0.0;
        for (int i = 0; i <= maxOverlappingLevel; i++)
        {
            double sscScore, olScore;
            {
                // For the level, let's figure out what the number of files should (optimally) be
                long optimalSSTableNumber = maxBytesForLevel(i, maxSSTableSizeInBytes) / maxSSTableSizeInBytes;
                sscScore = ((double) getLevelSize(i)) / optimalSSTableNumber;
                if (sscScore > sstableCountScore)
                {
                    sstableCountScore = sscScore;
                    sstableCountLevel = i;
                }
            }

            {
                olScore = calculateOverlappingScore(i);
                if (olScore > overlappingScore)
                {
                    overlappingScore = olScore;
                    overlappingLevel = i;
                }
            }
            logger.debug("Overlapping scores for {} L{}: sscScore {}, olScore {}", tableId, i, sscScore, olScore);
        }

        if (overlappingScore > 0)
        {
            Collection<SSTableReader> candidates = getCandidatesForSameLevelCompaction(overlappingLevel, MAX_COMPACTING_OVERLAPPING * maxSSTableSizeInBytes);
            if (candidates.isEmpty())
            {
                return null;
            }

            // We have to at least compact to L1
            int compactToLevel = 1;
            for (SSTableReader sstable : candidates)
            {
                if (sstable.getSSTableLevel() > compactToLevel)
                    compactToLevel = sstable.getSSTableLevel();
            }
            return new CompactionCandidate(candidates, compactToLevel, cfs.getCompactionStrategyManager().getMaxSSTableBytes());
        }
        else if (sstableCountScore > 1.001)
        {
            Collection<SSTableReader> candidates = getCandidatesForUplevelCompaction(sstableCountLevel);
            if (candidates.isEmpty())
            {
                return null;
            }

            return new CompactionCandidate(candidates, getNextLevel(candidates), cfs.getCompactionStrategyManager().getMaxSSTableBytes());
        }

        return null;
    }

    private double calculateOverlappingScore(int level)
    {
        List<SSTableReader> readers = new ArrayList<>(getLevel(level));
        Collections.sort(readers, SSTableReader.sstableComparator);
        final Set<SSTableReader> compacting = cfs.getTracker().getCompacting();
        SSTableReader last = null;
        int overlaps = 0, possibleOverlaps = 0;
        for (SSTableReader reader: readers)
        {
            // We only want to count a possible overlap when the SSTable's in question are not being compacted.
            if (!overlapping(reader, compacting).isEmpty())
                continue;

            if (last != null)
            {
                if (last.last.compareTo(reader.first) > 0) {
                    overlaps++;
                }
            }
            last = reader;
            possibleOverlaps++;
        }
        double overlapScore = ((double)overlaps)/possibleOverlaps;
        overlapScore = Double.isNaN(overlapScore) ? 0.0 : overlapScore;
        if (logger.isDebugEnabled())
            logger.debug("{} L{} overlap score: {}", tableId, level, overlapScore);
        return overlapScore;
    }

    private List<SSTableReader> getSSTablesForSTCS(Collection<SSTableReader> sstables)
    {
        Iterable<SSTableReader> candidates = cfs.getTracker().getUncompacting(sstables);
        List<Pair<SSTableReader,Long>> pairs = SizeTieredCompactionStrategy.createSSTableAndLengthPairs(AbstractCompactionStrategy.filterSuspectSSTables(candidates));
        List<List<SSTableReader>> buckets = SizeTieredCompactionStrategy.getBuckets(pairs,
                                                                                    options.bucketHigh,
                                                                                    options.bucketLow,
                                                                                    options.minSSTableSize);
        return SizeTieredCompactionStrategy.mostInterestingBucket(buckets, cfs.getMinimumCompactionThreshold(), cfs.getMaximumCompactionThreshold());
    }

    /**
     * If we do something that makes many levels contain too little data (cleanup, change sstable size) we will "never"
     * compact the high levels.
     *
     * This method finds if we have gone many compaction rounds without doing any high-level compaction, if so
     * we start bringing in one sstable from the highest level until that level is either empty or is doing compaction.
     *
     * @param targetLevel the level the candidates will be compacted into
     * @param candidates the original sstables to compact
     * @return
     */
    private Collection<SSTableReader> getOverlappingStarvedSSTables(int targetLevel, Collection<SSTableReader> candidates)
    {
        Set<SSTableReader> withStarvedCandidate = new HashSet<>(candidates);

        for (int i = generations.length - 1; i > 0; i--)
            compactionCounter[i]++;
        compactionCounter[targetLevel] = 0;
        if (logger.isTraceEnabled())
        {
            for (int j = 0; j < compactionCounter.length; j++)
                logger.trace("CompactionCounter({}): {}: {}", tableId, j, compactionCounter[j]);
        }

        for (int i = generations.length - 1; i > 0; i--)
        {
            if (getLevelSize(i) > 0)
            {
                if (compactionCounter[i] > NO_COMPACTION_LIMIT)
                {
                    // we try to find an sstable that is fully contained within  the boundaries we are compacting;
                    // say we are compacting 3 sstables: 0->30 in L1 and 0->12, 12->33 in L2
                    // this means that we will not create overlap in L2 if we add an sstable
                    // contained within 0 -> 33 to the compaction
                    PartitionPosition max = null;
                    PartitionPosition min = null;
                    for (SSTableReader candidate : candidates)
                    {
                        if (min == null || candidate.first.compareTo(min) < 0)
                            min = candidate.first;
                        if (max == null || candidate.last.compareTo(max) > 0)
                            max = candidate.last;
                    }
                    if (min == null || max == null || min.equals(max)) // single partition sstables - we cannot include a high level sstable.
                        return candidates;
                    Set<SSTableReader> compacting = cfs.getTracker().getCompacting();
                    Range<PartitionPosition> boundaries = new Range<>(min, max);
                    for (SSTableReader sstable : getLevel(i))
                    {
                        Range<PartitionPosition> r = new Range<PartitionPosition>(sstable.first, sstable.last);
                        if (boundaries.contains(r) && !compacting.contains(sstable))
                        {
                            logger.info("Adding high-level ({} L{}) {} to candidates", tableId, sstable.getSSTableLevel(), sstable);
                            withStarvedCandidate.add(sstable);
                            return withStarvedCandidate;
                        }
                    }
                }
                return candidates;
            }
        }

        return candidates;
    }

    public synchronized int getLevelSize(int i)
    {
        if (i >= generations.length)
            throw new ArrayIndexOutOfBoundsException("Maximum valid generation is " + (generations.length - 1));
        return getLevel(i).size();
    }

    public synchronized int[] getAllLevelSize()
    {
        int[] counts = new int[generations.length];
        for (int i = 0; i < counts.length; i++)
            counts[i] = getLevel(i).size();
        return counts;
    }

    private void logDistribution()
    {
        if (logger.isTraceEnabled())
        {
            for (int i = 0; i < generations.length; i++)
            {
                if (!getLevel(i).isEmpty())
                {
                    logger.trace("{} L{} contains {} SSTables ({} bytes) in {}",
                                 tableId, i, getLevel(i).size(), SSTableReader.getTotalBytes(getLevel(i)), this);
                }
            }
        }
    }

    @VisibleForTesting
    public int remove(SSTableReader reader)
    {
        int level = reader.getSSTableLevel();
        assert level >= 0 : reader + " not present in manifest: "+level;
        generations[level].remove(reader);
        return level;
    }

    private static Set<SSTableReader> overlapping(Collection<SSTableReader> candidates, Iterable<SSTableReader> others)
    {
        assert !candidates.isEmpty();
        /*
         * Picking each sstable from others that overlap one of the sstable of candidates is not enough
         * because you could have the following situation:
         *   candidates = [ s1(a, c), s2(m, z) ]
         *   others = [ s3(e, g) ]
         * In that case, s2 overlaps none of s1 or s2, but if we compact s1 with s2, the resulting sstable will
         * overlap s3, so we must return s3.
         *
         * Thus, the correct approach is to pick sstables overlapping anything between the first key in all
         * the candidate sstables, and the last.
         */
        Iterator<SSTableReader> iter = candidates.iterator();
        SSTableReader sstable = iter.next();
        Token first = sstable.first.getToken();
        Token last = sstable.last.getToken();
        while (iter.hasNext())
        {
            sstable = iter.next();
            first = first.compareTo(sstable.first.getToken()) <= 0 ? first : sstable.first.getToken();
            last = last.compareTo(sstable.last.getToken()) >= 0 ? last : sstable.last.getToken();
        }
        return overlapping(first, last, others);
    }

    @VisibleForTesting
    static Set<SSTableReader> overlapping(SSTableReader sstable, Iterable<SSTableReader> others)
    {
        return overlapping(sstable.first.getToken(), sstable.last.getToken(), others);
    }

    private Set<SSTableReader> padCandidates(Set<SSTableReader> candidates, int level, int maxSize)
    {
        if (candidates.size() >= maxSize)
            return candidates;

        final Set<SSTableReader> compacting = cfs.getTracker().getCompacting();
        Collection<SSTableReader> uplevelCandidates = getLevel(level + 1);
        Collection<SSTableReader> filteredCandidates = Lists.newArrayList(Iterables.filter(uplevelCandidates, Predicates.not(Predicates.or(Predicates.in(compacting), suspectP))));
        logger.trace("padding candidates for {} L{} compaction up to {} from {}; L{} has {}/{} candidates", tableId, level, maxSize, candidates.size(), level + 1, filteredCandidates.size(), uplevelCandidates.size());

        Set<SSTableReader> potentialInclusions = overlapping(candidates, filteredCandidates);
        if (potentialInclusions.size() + candidates.size() <= maxSize)
        {
            logger.trace("found {} padding candidates for {} L{} compaction", potentialInclusions.size(), tableId, level);
            return Sets.union(candidates, potentialInclusions);
        }
        else
        {
            logger.trace("fully padded {} L{} compaction", tableId, level);
            return Sets.union(candidates, new HashSet<>(ageSortedSSTables(potentialInclusions).subList(0, maxSize - candidates.size())));
        }
    }

    /**
     * @return sstables from @param sstables that contain keys between @param start and @param end, inclusive.
     */
    private static Set<SSTableReader> overlapping(Token start, Token end, Iterable<SSTableReader> sstables)
    {
        assert start.compareTo(end) <= 0;
        Set<SSTableReader> overlapped = new HashSet<>();
        Bounds<Token> promotedBounds = new Bounds<Token>(start, end);
        for (SSTableReader candidate : sstables)
        {
            Bounds<Token> candidateBounds = new Bounds<Token>(candidate.first.getToken(), candidate.last.getToken());
            if (candidateBounds.intersects(promotedBounds))
                overlapped.add(candidate);
        }
        return overlapped;
    }

    private static final Predicate<SSTableReader> suspectP = new Predicate<SSTableReader>()
    {
        public boolean apply(SSTableReader candidate)
        {
            return candidate.isMarkedSuspect();
        }
    };

    /**
     * Select some of the candidate SSTables which can be used which are maximally overlapping
     *
     * Try to select up to maxCompactingBytes of SSTables to be compacted.
     * @param level              Level to look at when selecting the SSTables to compact
     * @param maxCompactingBytes Maximal number of bytes to include in this compaction
     * @return
     */
    private Collection<SSTableReader> getCandidatesForSameLevelCompaction(int level, long maxCompactingBytes)
    {
        assert level <= maxOverlappingLevel : "Cannot run a same level compaction for a level which is not overlapping.";
        logger.debug("Choosing candidates for same level at {} L{}", tableId, level);
        final Set<SSTableReader> compacting = cfs.getTracker().getCompacting();
        Set<SSTableReader> candidates = new HashSet<>();

        Collections.sort(getLevel(level), SSTableReader.sstableComparator);

        int start = 0;
        for (int i = 0; i < getLevel(level).size(); i++)
        {
            SSTableReader sstable = getLevel(level).get(i);
            if (sstable.first.compareTo(lastCompactedKeys[level]) > 0)
            {
                start = i;
                break;
            }
        }

        SSTableReader last = null;
        for (int i = 0; i < getLevel(level).size(); i++)
        {
            SSTableReader newCandidate = getLevel(level).get((i + start) % getLevel(level).size());

            if (last != null)
            {
                if (last.last.compareTo(newCandidate.first) > 0)
                {
                    if (!candidates.contains(last))
                    {
                        candidates.add(last);
                        maxCompactingBytes -= last.onDiskLength();
                    }
                    candidates.add(newCandidate);
                    maxCompactingBytes -= newCandidate.onDiskLength();
                }
            }
            if (maxCompactingBytes < 0)
                break;
            last = newCandidate;
        }

        if (candidates.isEmpty())
        {
            return Collections.emptyList();
        }

        if (level == maxOverlappingLevel)
        {
            Collection<SSTableReader> nonOverlappingLevel = getLevel(level + 1);

            candidates = Sets.union(candidates, overlapping(candidates, nonOverlappingLevel));
 
            if (!overlapping(candidates, compacting).isEmpty())
            {
                candidates = Collections.emptySet();
            }
        }

        if (candidates.size() <= 1)
            return Collections.emptyList();

        return padCandidates(candidates, level, MAX_COMPACTING_OVERLAPPING);
    }

    /**
     * @return highest-priority sstables to compact for the given level.
     * If no compactions are possible (because of concurrent compactions or because some sstables are blacklisted
     * for prior failure), will return an empty list.  Never returns null.
     */
    private Collection<SSTableReader> getCandidatesForUplevelCompaction(int level)
    {
        assert !getLevel(level).isEmpty();
        logger.debug("Choosing candidates for up level compaction at {} L{}", tableId, level);

        final Set<SSTableReader> compacting = cfs.getTracker().getCompacting();

        // When we are promoting, we are trying to accomplish two different goals:
        // - Produce a shorter range for each of the files to be searched
        // - Reduce the amount of garbage that must be searched in the files
        //
        // As such, we try to push files so that each file covers a smaller range than in the current level.
        // We must look at the level to which we are going to be pushing the files in order to determine what we are looking for in the cohort.
        //
        // If we are pushing to a level which allows overlap, we are trying to get as many files from the current level together as possible. If we don't hit MAX_COMPACTING_OVERLAPPING, we include some from the level up.
        // If we are pushing to a level which does not allow overlap, we are trying to pull all of the files which overlap with the next level as possible while including as few of the next level as possible.
        // If we are pushing *from* L0, we want to take the files which have been hanging around the longest, because *all* of the files will overlap as much as the next.
        if (level == 0)
        {
            logger.trace("{} L0 check", tableId);
            Set<SSTableReader> compactingL0 = getCompacting(0);

            Set<SSTableReader> candidates = new HashSet<>();
            Set<SSTableReader> remaining = new HashSet<>();

            Iterables.addAll(remaining, Iterables.filter(getLevel(0), Predicates.not(suspectP)));

            int possibles = remaining.size();
            if (maxOverlappingLevel != 0)
            {
                for (SSTableReader sstable : ageSortedSSTables(remaining))
                {
                    if (candidates.contains(sstable))
                        continue;

                    if (compactingL0.contains(sstable))
                        continue;

                    Sets.SetView<SSTableReader> overlappedL0 = Sets.union(Collections.singleton(sstable), overlapping(sstable, remaining));

                    for (SSTableReader newCandidate : overlappedL0)
                    {
                        if (!compactingL0.contains(newCandidate))
                            candidates.add(newCandidate);

                        remaining.remove(newCandidate);
                    }

                    if (candidates.size() > MAX_COMPACTING_OVERLAPPING)
                    {
                        // limit to only the MAX_COMPACTING_OVERLAPPING oldest candidates
                        candidates = new HashSet<>(ageSortedSSTables(candidates).subList(0, MAX_COMPACTING_OVERLAPPING));
                        break;
                    }
                }
                logger.trace("Considered {} for {} L0 overlapping compaction, but was filtered down to {}", possibles, tableId, candidates.size());

                // leave everything in L0 if we didn't end up with a full sstable's worth of data
                if (SSTableReader.getTotalBytes(candidates) < maxSSTableSizeInBytes)
                {
                    return Collections.emptyList();
                }

                if (candidates.size() > MAX_COMPACTING_OVERLAPPING)
                {
                    // limit to only the MAX_COMPACTING_OVERLAPPING oldest candidates
                    candidates = new HashSet<>(ageSortedSSTables(candidates).subList(0, MAX_COMPACTING_OVERLAPPING));
                }
                else if (candidates.size() > 1)
                {
                    candidates = padCandidates(candidates, 0, MAX_COMPACTING_OVERLAPPING);
                }
            }
            else
            {
                PartitionPosition lastCompactingKey = null;
                PartitionPosition firstCompactingKey = null;

                for (SSTableReader candidate : compactingL0)
                {
                    if (firstCompactingKey == null || candidate.first.compareTo(firstCompactingKey) < 0)
                        firstCompactingKey = candidate.first;
                    if (lastCompactingKey == null || candidate.last.compareTo(lastCompactingKey) > 0)
                        lastCompactingKey = candidate.last;
                }

                for (SSTableReader sstable : ageSortedSSTables(remaining))
                {
                    if (candidates.contains(sstable))
                        continue;

                    Sets.SetView<SSTableReader> overlappedL0 = Sets.union(Collections.singleton(sstable), overlapping(sstable, remaining));

                    if (!overlapping(overlappedL0, compactingL0).isEmpty())
                        continue;

                    for (SSTableReader newCandidate : overlappedL0)
                    {
                        if (firstCompactingKey == null || lastCompactingKey == null || overlapping(firstCompactingKey.getToken(), lastCompactingKey.getToken(), Arrays.asList(newCandidate)).size() == 0)
                            candidates.add(newCandidate);
                        remaining.remove(newCandidate);
                    }

                    if (candidates.size() > MAX_COMPACTING_OVERLAPPING)
                    {
                        // limit to only the MAX_COMPACTING_OVERLAPPING oldest candidates
                        candidates = new HashSet<>(ageSortedSSTables(candidates).subList(0, MAX_COMPACTING_OVERLAPPING));
                        break;
                    }
                }
                logger.trace("Considered {} for {} L0 nonoverlapping compaction, but was filtered down to {}", possibles, tableId, candidates.size());

                // leave everything in L0 if we didn't end up with a full sstable's worth of data
                if (SSTableReader.getTotalBytes(candidates) < maxSSTableSizeInBytes)
                {
                    return Collections.emptyList();
                }

                Set<SSTableReader> levelOverlapping = overlapping(candidates, getLevel(level + 1));
                if (Sets.intersection(levelOverlapping, compacting).size() > 0)
                    return Collections.emptyList();
                candidates = Sets.union(candidates, levelOverlapping);
            }

            if (candidates.size() > 1)
                return candidates;
        }
        else if (level < maxOverlappingLevel)
        {
            logger.trace("{} L{} MOLO check", tableId, level);
            // for non-overlapping compactions, pick up where we left off last time
            Collections.sort(getLevel(level), SSTableReader.sstableComparator);

            int start = 0; // handles case where the prior compaction touched the very last range
            for (int i = 0; i < getLevel(level).size(); i++)
            {
                SSTableReader sstable = getLevel(level).get(i);
                if (sstable.first.compareTo(lastCompactedKeys[level]) > 0)
                {
                    start = i;
                    break;
                }
            }

            Set<SSTableReader> compactingLevel = ImmutableSet.copyOf(Iterables.filter(getLevel(level), Predicates.in(compacting)));
            Set<SSTableReader> compactingUplevel = ImmutableSet.copyOf(Iterables.filter(getLevel(level + 1), Predicates.in(compacting)));

            // look for a non-suspect keyspace to compact with, starting with where we left off last time,
            // and wrapping back to the beginning of the generation if necessary
            Set<SSTableReader> candidates = new HashSet<>();
            Set<SSTableReader> remaining = new HashSet<>();
            Set<SSTableReader> uplevelRemaining = new HashSet<>();

            Iterables.addAll(remaining, Iterables.filter(getLevel(level), Predicates.and(Predicates.not(Predicates.in(compactingLevel)), Predicates.not(suspectP))));
            Iterables.addAll(uplevelRemaining, Iterables.filter(getLevel(level + 1), Predicates.and(Predicates.not(Predicates.in(compactingUplevel)), Predicates.not(suspectP))));

            for (int i = 0; i < getLevel(level).size(); i++)
            {
                SSTableReader sstable = getLevel(level).get((start + i) % getLevel(level).size());
                // if it isn't in remaining it was either already included, or it is compacting
                if (!remaining.contains(sstable) || sstable.isMarkedSuspect())
                    continue;

                Set<SSTableReader> overlapping = overlapping(Collections.singleton(sstable), remaining);
                for (SSTableReader candidate : overlapping)
                {
                    candidates.add(candidate);
                    remaining.remove(candidate);
                }

                Set<SSTableReader> uplevelOverlapping = overlapping(candidates, uplevelRemaining);
                for (SSTableReader candidate : uplevelOverlapping)
                {
                    candidates.add(candidate);
                    uplevelRemaining.remove(candidate);
                }

                if (candidates.size() > MAX_COMPACTING_OVERLAPPING)
                {
                    // limit to only the MAX_COMPACTING_OVERLAPPING oldest candidates
                    candidates = new HashSet<>(ageSortedSSTables(candidates).subList(0, MAX_COMPACTING_OVERLAPPING));
                    break;
                }
            }

            if (candidates.size() > 1)
                return padCandidates(candidates, level, MAX_COMPACTING_OVERLAPPING);
        }
        else
        {
            logger.trace("{} L{} unoverlapping check", tableId, level);
            // for non-overlapping compactions, pick up where we left off last time
            Collections.sort(getLevel(level), SSTableReader.sstableComparator);
            int start = 0; // handles case where the prior compaction touched the very last range
            for (int i = 0; i < getLevel(level).size(); i++)
            {
                SSTableReader sstable = getLevel(level).get(i);
                if (sstable.first.compareTo(lastCompactedKeys[level]) > 0)
                {
                    start = i;
                    break;
                }
            }

            // look for a non-suspect keyspace to compact with, starting with where we left off last time,
            // and wrapping back to the beginning of the generation if necessary
            for (int i = 0; i < getLevel(level).size(); i++)
            {
                SSTableReader sstable = getLevel(level).get((start + i) % getLevel(level).size());

                // Also take any overlapping sstables; there will only be overlaps when level = maxOverlappingLevel
                Set<SSTableReader> levelCandidates = Sets.union(Collections.singleton(sstable), overlapping(sstable, getLevel(level)));
                Set<SSTableReader> candidates = Sets.union(levelCandidates, overlapping(levelCandidates, getLevel(level + 1)));
                if (Iterables.any(candidates, suspectP))
                    continue;
                if (Sets.intersection(candidates, compacting).isEmpty())
                    return candidates;
            }

            // all the sstables were suspect or overlapped with something suspect
        }

        return Collections.emptyList();
    }

    private Set<SSTableReader> getCompacting(int level)
    {
        Set<SSTableReader> sstables = new HashSet<>();
        Set<SSTableReader> levelSSTables = new HashSet<>(getLevel(level));
        for (SSTableReader sstable : cfs.getTracker().getCompacting())
        {
            if (levelSSTables.contains(sstable))
                sstables.add(sstable);
        }
        return sstables;
    }

    private List<SSTableReader> ageSortedSSTables(Collection<SSTableReader> candidates)
    {
        List<SSTableReader> ageSortedCandidates = new ArrayList<>(candidates);
        Collections.sort(ageSortedCandidates, SSTableReader.maxTimestampComparator);
        return ageSortedCandidates;
    }

    @Override
    public String toString()
    {
        return "Manifest@" + hashCode();
    }

    public int getLevelCount()
    {
        for (int i = generations.length - 1; i >= 0; i--)
        {
            if (!getLevel(i).isEmpty())
                return i;
        }
        return 0;
    }

    public synchronized SortedSet<SSTableReader> getLevelSorted(int level, Comparator<SSTableReader> comparator)
    {
        return ImmutableSortedSet.copyOf(comparator, getLevel(level));
    }

    public List<SSTableReader> getLevel(int i)
    {
        return generations[i];
    }

    public Iterable<String> getSets()
    {
        List<String> levels = new ArrayList<>();
        for (int i = 0; i <= getLevelCount(); i++)
        {
            levels.add("L" + i);
        }
        return levels;
    }


    public List<String> getSSTables(String set)
    {
        List<SSTableReader> generation = getLevel(Integer.parseInt(set.substring(1)));
        List<String> sstables = new ArrayList<>(generation.size());
        for (int i = 0; i < generation.size(); i++)
        {
            String filename = generation.get(i).getFilename();
            sstables.add(i, filename.substring(filename.lastIndexOf('/') + 1));
        }
        return sstables;
    }

    public synchronized int getEstimatedTasks()
    {
        long tasks = 0;
        long[] estimated = new long[generations.length];

        for (int i = 0; i <= maxOverlappingLevel; i++)
        {
            List<SSTableReader> sstables = getLevel(i);
            // If there is 1 byte over TBL - (MBL * 1.001), there is still a task left, so we need to round up.
            long sizeEstimate = (long)Math.ceil((double)Math.max(0L, SSTableReader.getTotalBytes(sstables) - (long)(maxBytesForLevel(i, maxSSTableSizeInBytes) * 1.001)) / (double)maxSSTableSizeInBytes);
            long overlapEstimate = (long)Math.ceil(calculateOverlappingScore(i));
            estimated[i] = sizeEstimate + overlapEstimate;
            tasks += estimated[i];
        }
        for (int i = generations.length - 1; i > maxOverlappingLevel; i--)
        {
            List<SSTableReader> sstables = getLevel(i);
            // If there is 1 byte over TBL - (MBL * 1.001), there is still a task left, so we need to round up.
            estimated[i] = (long)Math.ceil((double)Math.max(0L, SSTableReader.getTotalBytes(sstables) - (long)(maxBytesForLevel(i, maxSSTableSizeInBytes) * 1.001)) / (double)maxSSTableSizeInBytes);
            tasks += estimated[i];
        }

        logger.trace("Estimating {} compactions to do for {}",
                     Arrays.toString(estimated), tableId);
        return Ints.checkedCast(tasks);
    }

    public int getNextLevel(Collection<SSTableReader> sstables)
    {
        int maximumLevel = Integer.MIN_VALUE;
        int minimumLevel = Integer.MAX_VALUE;
        for (SSTableReader sstable : sstables)
        {
            maximumLevel = Math.max(sstable.getSSTableLevel(), maximumLevel);
            minimumLevel = Math.min(sstable.getSSTableLevel(), minimumLevel);
        }

        int newLevel;
        if (minimumLevel == 0 && minimumLevel == maximumLevel && SSTableReader.getTotalBytes(sstables) < maxSSTableSizeInBytes)
        {
            newLevel = 0;
        }
        else
        {
            newLevel = minimumLevel == maximumLevel ? maximumLevel + 1 : maximumLevel;
            assert newLevel > 0;
        }
        return newLevel;

    }

    public Iterable<SSTableReader> getAllSSTables()
    {
        Set<SSTableReader> sstables = new HashSet<>();
        for (List<SSTableReader> generation : generations)
        {
            sstables.addAll(generation);
        }
        return sstables;
    }

    public static class CompactionCandidate
    {
        public final Collection<SSTableReader> sstables;
        public final int level;
        public final long maxSSTableBytes;

        public CompactionCandidate(Collection<SSTableReader> sstables, int level, long maxSSTableBytes)
        {
            this.sstables = sstables;
            this.level = level;
            this.maxSSTableBytes = maxSSTableBytes;
        }
    }
}
