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
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.NoSpamLogger;

public class CompactionLogger implements CompactionStrategyLogger.Conduit
{
    private interface SimpleLogger
    {
        void begin(CompactionStrategyLogger.Conduit conduit);

        void end(CompactionStrategyLogger.Conduit conduit);

        void startCompaction(CompactionStrategyLogger.Conduit conduit, CompactionTask task);

        void finishCompaction(CompactionStrategyLogger.Conduit conduit, CompactionTask task);

        void format(CompactionStrategyLogger.Conduit conduit, SSTableReader reader);
    }

    private static final Logger logger = LoggerFactory.getLogger(CompactionLogger.class);
    private static final CompactionLogSerializer serializer = new CompactionLogSerializer();
    private static final ConcurrentHashMap<AbstractCompactionStrategy, CompactionLogger> loggers = new ConcurrentHashMap<>();
    private final Enabled enabled;
    private final String keyspace;
    private final String table;
    private final String strategyName;
    private final SimpleLogger strategyLogger;
    private final ColumnFamilyStore cfs;

    private String managerType;
    private boolean isEnabled = false;
    private boolean toLog = false;
    private CompactionLogStatement currentStatement;

    private CompactionLogger(Enabled enabled,
                             ColumnFamilyStore cfs,
                             String keyspace,
                             String table,
                             SimpleLogger strategyLogger,
                             String strategyName)
    {
        this.enabled = enabled;
        this.cfs = cfs;
        this.keyspace = keyspace;
        this.table = table;
        this.strategyLogger = strategyLogger;
        this.strategyName = strategyName;
    }

    @SuppressWarnings("unchecked")
    private static SimpleLogger wrapLogger(AbstractCompactionStrategy strategy)
    {
        CompactionStrategyLogger logger = strategy.getLogger();
        return new SimpleLogger()
        {
            public void begin(CompactionStrategyLogger.Conduit conduit)
            {
                logger.begin(conduit, strategy);
            }

            public void end(CompactionStrategyLogger.Conduit conduit)
            {
                logger.end(conduit, strategy);
            }

            public void startCompaction(CompactionStrategyLogger.Conduit conduit, CompactionTask task)
            {
                logger.startCompaction(conduit, strategy, task);
            }

            public void finishCompaction(CompactionStrategyLogger.Conduit conduit, CompactionTask task)
            {
                logger.finishCompaction(conduit, strategy, task);
            }

            public void format(CompactionStrategyLogger.Conduit conduit, SSTableReader reader)
            {
                logger.format(conduit, strategy, reader);
            }
        };
    }

    public static CompactionLogger getLogger(AbstractCompactionStrategy strategy, boolean ensureEnabled)
    {
        return loggers.computeIfAbsent(strategy, acs -> {
            ColumnFamilyStore cfs = strategy.cfs;

            Enabled enabled = Enabled.getEnabled(cfs);
            if (ensureEnabled && !enabled.isEnabled())
                enabled.enable();
            
            CompactionLogger logger = new CompactionLogger(enabled,
                                                           cfs,
                                                           cfs.keyspace.getName(),
                                                           cfs.getTableName(),
                                                           wrapLogger(strategy),
                                                           strategy.getName());
            enabled.addDependent(logger);
            return logger;
        });
    }

    public void setManagerType(String managerType)
    {
        this.managerType = managerType;
        if (toLog)
        {
            logStart();
            toLog = false;
        }
    }

    private void capture(CompactionLogStatement compactionLogStatement)
    {
        if (currentStatement != null)
            flush();
        currentStatement = compactionLogStatement;
    }

    private void flush()
    {
        serializer.write(currentStatement);
        currentStatement = null;
    }

    public void startNewMessage()
    {
        CompactionLogStatement next = currentStatement.createStrategySpecific();
        if (next == null)
        {
            next = new CompactionLogStatement.StrategyUnknown(keyspace, table, managerType);
        }
        capture(next);
    }

    public void add(Object value)
    {
        assert currentStatement != null : "Cannot add to a non-existant statement; flush called too soon?";
        currentStatement.add(value);
    }

    private void enable()
    {
        isEnabled = true;
        if (managerType != null)
        {
            logStart();
        }
        else
        {
            toLog = true;
        }
    }

    private void logStart()
    {
        capture(new CompactionLogStatement.LogStart(keyspace, table, managerType, strategyName));
        this.strategyLogger.begin(this);

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if ((sstable.isRepaired() && managerType.equals("repaired"))
                || (!sstable.isRepaired() && managerType.equals("unrepaired")))
            {
                capture(new CompactionLogStatement.LogStartTable(keyspace, table, managerType, sstable));
                this.strategyLogger.format(this, sstable);
            }
        }

        flush();
    }

    private void disable()
    {
        if (isEnabled)
        {
            isEnabled = false;
            capture(new CompactionLogStatement.LogEnd(keyspace, table, managerType));
            this.strategyLogger.end(this);
            flush();
        }
    }

    public void compaction(CompactionTask task,
                           Iterable<SSTableReader> startTables, long startTime, long startSize,
                           Iterable<SSTableReader> endTables, long endTime, long endSize)
    {
        if (!isEnabled) return;

        UUID compactionId = task.transaction.opId();
        capture(new CompactionLogStatement.CompactionStart(keyspace, table, managerType, compactionId, startTime, startSize));
        strategyLogger.startCompaction(this, task);

        for (SSTableReader sstable : startTables)
        {
            capture(new CompactionLogStatement.CompactionStartTable(compactionId, sstable));
            strategyLogger.format(this, sstable);
        }

        capture(new CompactionLogStatement.CompactionEnd(compactionId, endTime, endSize));
        strategyLogger.finishCompaction(this, task);

        for (SSTableReader sstable : endTables)
        {
            capture(new CompactionLogStatement.CompactionEndTable(compactionId, sstable));
            strategyLogger.format(this, sstable);
        }
        flush();
    }

    public void completedFlush(Collection<SSTableReader> sstables)
    {
        if (!isEnabled) return;

        for (SSTableReader sstable : sstables)
        {
            capture(new CompactionLogStatement.Flush(keyspace, table, managerType, System.currentTimeMillis(), sstable));
            strategyLogger.format(this, sstable);
        }
        flush();
    }

    public void pending(long time, int count)
    {
        if (!isEnabled) return;

        capture(new CompactionLogStatement.PendingCompactions(keyspace, table, managerType, time, count));
        flush();
    }

    public void shutdown()
    {
        disable();
        enabled.removeDependent(this);
    }

    public static void enableFor(ColumnFamilyStore columnFamilyStore)
    {
        Enabled.getEnabled(columnFamilyStore).enable();
    }

    public static void disableFor(ColumnFamilyStore columnFamilyStore)
    {
        Enabled.getEnabled(columnFamilyStore).disable();
    }

    private static class Enabled
    {
        private static final Map<ColumnFamilyStore, Enabled> enableds = new ConcurrentHashMap<>();

        public static Enabled getEnabled(ColumnFamilyStore cfs)
        {
            return enableds.computeIfAbsent(cfs, columnFamilyStore -> new Enabled());
        }

        private final Set<CompactionLogger> loggers = new HashSet<>();

        private boolean enabled = false;

        public boolean isEnabled()
        {
            return enabled;
        }

        public synchronized void enable()
        {
            enabled = true;
            loggers.forEach(CompactionLogger::enable);
        }

        public synchronized void disable()
        {
            enabled = false;
            loggers.forEach(CompactionLogger::disable);
        }

        public synchronized void addDependent(CompactionLogger logger)
        {
            loggers.add(logger);
            if (enabled)
            {
                logger.enable();
            }
        }

        public synchronized void removeDependent(CompactionLogger logger)
        {
            loggers.remove(logger);
        }
    }

    /**
     * This makes sure that the values that go into the log are properly serialized.
     */
    private static class CompactionLogSerializer
    {
        private static final String logDirectory = System.getProperty("cassandra.logdir", ".");
        private final ExecutorService loggerService = Executors.newFixedThreadPool(1);
        private OutputStream stream;

        private static OutputStream createStream() throws IOException
        {
            int count = 0;
            Path compactionLog = Paths.get(logDirectory, "compaction.log");
            if (Files.exists(compactionLog))
            {
                Path tryPath = compactionLog;
                while (Files.exists(tryPath))
                {
                    tryPath = Paths.get(logDirectory, String.format("compaction-%d.log", count++));
                }
                Files.move(compactionLog, tryPath);
            }

            return Files.newOutputStream(compactionLog, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        }

        public void write(CompactionLogStatement statement)
        {
            final byte[] toWrite = statement.get();
            loggerService.execute(() -> {
                try
                {
                    if (stream == null)
                        stream = createStream();

                    stream.write(toWrite);
                }
                catch (IOException ioe)
                {
                    // We'll drop the change and log the error to the logger.
                    NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR, 1, TimeUnit.MINUTES,
                                     "Could not write to the log file: {}", ioe);
                }
            });
        }
    }
}
