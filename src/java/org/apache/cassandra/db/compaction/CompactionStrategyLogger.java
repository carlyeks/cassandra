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

import org.apache.cassandra.io.sstable.format.SSTableReader;

public interface CompactionStrategyLogger<T extends AbstractCompactionStrategy>
{
    interface Conduit {
        void startNewMessage();
        void add(Object value);
    }

    CompactionStrategyLogger NoLogger = new CompactionStrategyLogger<AbstractCompactionStrategy>()
    {
        public void begin(Conduit conduit, AbstractCompactionStrategy strategy)
        {
        }

        public void end(Conduit conduit, AbstractCompactionStrategy strategy)
        {
        }

        public void startCompaction(Conduit conduit, AbstractCompactionStrategy strategy, CompactionTask task)
        {
        }

        public void finishCompaction(Conduit conduit, AbstractCompactionStrategy strategy, CompactionTask task)
        {
        }

        public void format(Conduit conduit, AbstractCompactionStrategy strategy, SSTableReader reader)
        {
        }
    };

    void begin(Conduit conduit, T strategy);

    void end(Conduit conduit, T strategy);

    void startCompaction(Conduit conduit, T strategy, CompactionTask task);

    void finishCompaction(Conduit conduit, T strategy, CompactionTask task);

    void format(Conduit conduit, T strategy, SSTableReader reader);
}
