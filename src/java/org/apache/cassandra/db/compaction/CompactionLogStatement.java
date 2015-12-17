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

import java.util.UUID;
import java.util.function.Consumer;

import org.apache.cassandra.io.sstable.format.SSTableReader;

public abstract class CompactionLogStatement
{
    public enum Type
    {
        LOG_START("START"),
        LOG_START_TABLE("STARTTABLE"),
        LOG_END("END"),
        COMPACTION_START("COMPACTSTART"),
        COMPACTION_START_TABLE("COMPACTSTARTTABLE"),
        COMPACTION_END("COMPACTEND"),
        COMPACTION_END_TABLE("COMPACTENDTABLE"),
        FLUSH("FLUSH"),
        LOG_START_STRATEGY("STARTSTRATEGY"),
        LOG_END_STRATEGY("ENDSTRATEGY"),
        PENDING_COMPACTIONS("COMPACTPENDING"),
        STRATEGY_UNKNOWN("UNKOWNFROMSTRATEGY");

        private final String tag;

        Type(String tag)
        {
            this.tag = tag;
        }
    }

    private final StringBuilder builder = new StringBuilder();

    public byte[] get() {
        builder.append(System.lineSeparator());
        return builder.toString().getBytes();
    }

    public CompactionLogStatement createStrategySpecific()
    {
        return null;
    }

    protected CompactionLogStatement(Type type)
    {
        builder.append(type.tag);
    }

    public void add(Object value)
    {
        builder.append(':').append(value);
    }

    private abstract static class TableStatement extends CompactionLogStatement
    {
        protected TableStatement(Type type, SSTableReader reader, Consumer<Consumer<Object>> beforeTable)
        {
            super(type);
            beforeTable.accept(this::add);
            add(reader.descriptor.generation);
            add(reader.descriptor.version.getVersion());
            add(reader.onDiskLength());
            add(reader.first.getToken());
            add(reader.last.getToken());
        }
    }

    public static class LogStart extends CompactionLogStatement
    {
        private final String keyspace;
        private final String table;
        private final String managerType;

        public LogStart(String keyspace, String table, String managerType, String strategyName)
        {
            super(Type.LOG_START);
            add(keyspace);
            this.keyspace = keyspace;
            add(table);
            this.table = table;
            add(managerType);
            this.managerType = managerType;
            add(strategyName);
            add(System.currentTimeMillis());
        }
        public CompactionLogStatement createStrategySpecific()
        {
            return new LogStartStrategy(keyspace, table, managerType);
        }

    }

    public static class LogStartTable extends TableStatement
    {
        public LogStartTable(String keyspace, String table, String managerType, SSTableReader reader)
        {
            super(Type.LOG_START_TABLE, reader, add -> {
                add.accept(keyspace);
                add.accept(table);
                add.accept(managerType);
            });
        }

    }

    public static class LogEnd extends CompactionLogStatement
    {
        private final String keyspace;
        private final String table;

        private final String managerType;

        public LogEnd(String keyspace, String table, String managerType)
        {
            super(Type.LOG_END);
            add(keyspace);
            this.keyspace = keyspace;
            add(table);
            this.table = table;
            add(managerType);
            this.managerType = managerType;
        }
        public CompactionLogStatement createStrategySpecific()
        {
            return new LogEndStrategy(keyspace, table, managerType);
        }

    }

    public static class CompactionStart extends CompactionLogStatement
    {
        public CompactionStart(String keyspace, String table, String managerType, UUID compactionId, long startTime, long size)
        {
            super(Type.COMPACTION_START);
            add(keyspace);
            add(table);
            add(managerType);
            add(compactionId);
            add(startTime);
            add(size);
        }

    }

    public static class CompactionStartTable extends TableStatement
    {
        public CompactionStartTable(UUID compactionId, SSTableReader reader)
        {
            super(Type.COMPACTION_START_TABLE, reader, add -> add.accept(compactionId));
        }

    }

    public static class CompactionEnd extends CompactionLogStatement
    {
        public CompactionEnd(UUID compactionId, long endTime, long size)
        {
            super(Type.COMPACTION_END);
            add(compactionId);
            add(endTime);
            add(size);
        }

    }

    public static class CompactionEndTable extends TableStatement
    {
        public CompactionEndTable(final UUID compactionId, SSTableReader reader)
        {
            super(Type.COMPACTION_END_TABLE, reader, add -> add.accept(compactionId));
        }
    }

    public static class Flush extends TableStatement
    {
        public Flush(String keyspace, String table, String type, long time, SSTableReader reader)
        {
            super(Type.FLUSH, reader, add-> {
                add.accept(keyspace);
                add.accept(table);
                add.accept(type);
                add.accept(time);
            });
        }
    }

    public static class LogStartStrategy extends CompactionLogStatement
    {
        public LogStartStrategy(String keyspace, String table, String type)
        {
            super(Type.LOG_START_STRATEGY);
            add(keyspace);
            add(table);
            add(type);
        }
    }

    public static class LogEndStrategy extends CompactionLogStatement
    {
        public LogEndStrategy(String keyspace, String table, String type)
        {
            super(Type.LOG_END_STRATEGY);
            add(keyspace);
            add(table);
            add(type);
        }
    }

    public static class PendingCompactions extends CompactionLogStatement
    {
        public PendingCompactions(String keyspace, String table, String type, long time, int count)
        {
            super(Type.PENDING_COMPACTIONS);
            add(keyspace);
            add(table);
            add(type);
            add(time);
            add(count);
        }
    }

    public static class StrategyUnknown extends CompactionLogStatement
    {
        public StrategyUnknown(String keyspace, String table, String type)
        {
            super(Type.STRATEGY_UNKNOWN);
            add(keyspace);
            add(table);
            add(type);
        }
    }
}
