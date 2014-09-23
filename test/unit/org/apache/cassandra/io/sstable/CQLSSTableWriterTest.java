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
package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

public class CQLSSTableWriterTest
{
    @BeforeClass
    public static void setup() throws Exception
    {
        StorageService.instance.initServer();
    }


    private void runTableTest(File dataDir, String ks, String table, int start) throws Exception
    {
        String schema = String.format("CREATE TABLE %s.%s ("
                + "  k int PRIMARY KEY,"
                + "  v1 text,"
                + "  v2 int"
                + ")", ks, table);
        String insert = String.format("INSERT INTO %s.%s (k, v1, v2) VALUES (?, ?, ?)", ks, table);
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                .inDirectory(dataDir)
                .forTable(schema)
                .withPartitioner(StorageService.instance.getPartitioner())
                .using(insert).build();

        writer.addRow(start * 4, "test1", 24);
        writer.addRow(start * 4 + 1, "test2", null);
        writer.addRow(start * 4 + 2, "test3", 42);
        writer.addRow(ImmutableMap.<String, Object>of("k", start * 4 + 3, "v2", 12));
        writer.close();
    }

    @Test
    public void testUnsortedWriter() throws Exception
    {
        String KS = "cql_keyspace";
        String TABLE = "table1";

        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
        assert dataDir.mkdirs();

        runTableTest(dataDir, KS, TABLE, 0);

        SSTableLoader loader = new SSTableLoader(dataDir, new SSTableLoader.Client()
        {
            public void init(String keyspace)
            {
                for (Range<Token> range : StorageService.instance.getLocalRanges("cql_keyspace"))
                    addRangeForEndpoint(range, FBUtilities.getBroadcastAddress());
                setPartitioner(StorageService.getPartitioner());
            }

            public CFMetaData getCFMetaData(String keyspace, String cfName)
            {
                return Schema.instance.getCFMetaData(keyspace, cfName);
            }
        }, new OutputHandler.SystemOutput(false, false));

        loader.stream().get();

        UntypedResultSet rs = QueryProcessor.processInternal("SELECT * FROM cql_keyspace.table1;");
        assertEquals(4, rs.size());

        Iterator<UntypedResultSet.Row> iter = rs.iterator();
        UntypedResultSet.Row row;

        row = iter.next();
        assertEquals(0, row.getInt("k"));
        assertEquals("test1", row.getString("v1"));
        assertEquals(24, row.getInt("v2"));

        row = iter.next();
        assertEquals(1, row.getInt("k"));
        assertEquals("test2", row.getString("v1"));
        assertFalse(row.has("v2"));

        row = iter.next();
        assertEquals(2, row.getInt("k"));
        assertEquals("test3", row.getString("v1"));
        assertEquals(42, row.getInt("v2"));

        row = iter.next();
        assertEquals(3, row.getInt("k"));
        assertEquals(null, row.getBytes("v1")); // Using getBytes because we know it won't NPE
        assertEquals(12, row.getInt("v2"));
    }

    @Test
    public void testUsingMultipleTables() throws Exception
    {
        String KS = "cql_keyspace";
        String TABLE1 = "table1";
        String TABLE2 = "table2";

        File tempdir = Files.createTempDir();
        File dataDir1 = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE1);
        File dataDir2 = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE2);
        assert dataDir1.mkdirs();
        assert dataDir2.mkdirs();

        runTableTest(dataDir1, KS, TABLE1, 0);
        runTableTest(dataDir2, KS, TABLE2, 0);
    }

    @Test
    public void testSyncWithinPartition() throws Exception
    {
        // Check that the write respect the buffer size even if we only insert rows withing the same partition (#7360)
        // To do that simply, we use a writer with a buffer of 1MB, and write 2 rows in the same partition with a value
        // > 1MB and validate that this created more than 1 sstable.
        File tempdir = Files.createTempDir();
        String schema = "CREATE TABLE ks.test ("
                      + "  k int PRIMARY KEY,"
                      + "  v blob"
                      + ")";
        String insert = "INSERT INTO ks.test (k, v) VALUES (?, ?)";
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(tempdir)
                                                  .forTable(schema)
                                                  .withPartitioner(StorageService.instance.getPartitioner())
                                                  .using(insert)
                                                  .withBufferSizeInMB(1)
                                                  .build();

        ByteBuffer val = ByteBuffer.allocate(1024 * 1050);

        writer.addRow(0, val);
        writer.addRow(1, val);
        writer.close();

        FilenameFilter filterDataFiles = new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                return name.endsWith("-Data.db");
            }
        };
        assert tempdir.list(filterDataFiles).length > 1 : Arrays.toString(tempdir.list(filterDataFiles));
    }

    private class ConcurrentCQLSSTableWriter implements Runnable
    {
        private final File dataDir;
        private final String ks, table;
        private final int start;
        private Exception exception;

        public ConcurrentCQLSSTableWriter(File dataDir, String ks, String table, int start)
        {
            this.dataDir = dataDir;
            this.ks = ks;
            this.table = table;
            this.start = start;
        }


        @Override
        public void run()
        {
            try
            {
                runTableTest(dataDir, ks, table, start);
            }
            catch (Exception e)
            {
                this.exception = e;
            }
        }
    }

    @Test
    public void testConcurrentWriters() throws Exception
    {
        String KS = "cql_keyspace";
        String TABLE = "table1";

        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
        assert dataDir.mkdirs();

        int CONCURRENT_WRITERS = 10;
        ConcurrentCQLSSTableWriter[] writers = new ConcurrentCQLSSTableWriter[CONCURRENT_WRITERS];
        Thread[] threads = new Thread[CONCURRENT_WRITERS];
        for (int i = 0; i < CONCURRENT_WRITERS; i++)
        {
            ConcurrentCQLSSTableWriter writer = new ConcurrentCQLSSTableWriter(dataDir, KS, TABLE, i);
            writers[i] = writer;
            Thread thread = new Thread(writer);
            threads[i] = thread;
            thread.start();
        }

        for (int i = 0; i < CONCURRENT_WRITERS; i++)
        {
            threads[i].join(5000);
            assert !threads[i].isAlive() : "Thread did not exit within 1 second";
            if (writers[i].exception != null)
            {
                throw writers[i].exception;
            }
        }
        // TODO: read in the files and ensure they were not mangled

        SSTableLoader loader = new SSTableLoader(dataDir, new SSTableLoader.Client()
        {
            public void init(String keyspace)
            {
                for (Range<Token> range : StorageService.instance.getLocalRanges("cql_keyspace"))
                    addRangeForEndpoint(range, FBUtilities.getBroadcastAddress());
                setPartitioner(StorageService.getPartitioner());
            }

            public CFMetaData getCFMetaData(String keyspace, String cfName)
            {
                return Schema.instance.getCFMetaData(keyspace, cfName);
            }
        }, new OutputHandler.SystemOutput(false, false));

        loader.stream().get();
    }
}
