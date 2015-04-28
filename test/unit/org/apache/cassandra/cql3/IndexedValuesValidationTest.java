/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.junit.Assert.fail;

public class IndexedValuesValidationTest extends CQLTester
{
    // CASSANDRA-8280/8081
    // reject updates with indexed values where value > 64k
    @Test
    public void testIndexOnCompositeValueOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b int, c blob, PRIMARY KEY (a))");
        createIndex("CREATE INDEX ON %s(c)");
        failToInsertWithIndexedValueOver64k("INSERT INTO %s (a, b, c) VALUES (0, 0, ?)");
    }

    @Test
    public void testIndexOnClusteringValueOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b blob, c int, PRIMARY KEY (a, b))");
        createIndex("CREATE INDEX ON %s(b)");
        failToInsertWithIndexedValueOver64k("INSERT INTO %s (a, b, c) VALUES (0, ?, 0)");
    }

    @Test
    public void testIndexOnPartitionKeyOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a blob, b int, c int, PRIMARY KEY ((a, b)))");
        createIndex("CREATE INDEX ON %s(a)");
        failToInsertWithIndexedValueOver64k("INSERT INTO %s (a, b, c) VALUES (?, 0, 0)");
    }

    @Test
    public void testCompactTableWithValueOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b blob, PRIMARY KEY (a)) WITH COMPACT STORAGE");
        createIndex("CREATE INDEX ON %s(b)");
        failToInsertWithIndexedValueOver64k("INSERT INTO %s (a, b) VALUES (0, ?)");
    }

    @Test
    public void testIndexOnClusteringValueInsertPartitionKeyOver64k() throws Throwable
    {
        // This test should fail because although none of the indexed
        // values are oversized, the indexed key would be.
        createTable("CREATE TABLE %s(a blob, b int, c int, PRIMARY KEY (a, b))");
        createIndex("CREATE INDEX ON %s(b)");

        failToInsertWithIndexedValueOver64k("INSERT INTO %s (a, b, c) VALUES (?, 0, 0)");
    }

    @Test
    public void testIndexOnCollectionInsertCollectionKeyOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b map<blob, int>, PRIMARY KEY (a))");
        createIndex("CREATE INDEX ON %s(b)");
        failToInsertWithIndexedValueOver64k("UPDATE %s SET b[?] = 0 WHERE a = 0");
    }

    @Test
    public void testIndexOnPartitionKeyInsertValueOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b int, c blob, PRIMARY KEY ((a, b)))");
        createIndex("CREATE INDEX ON %s(a)");
        succeedInsertWithIndexedValueOver64k("INSERT INTO %s (a, b, c) VALUES (0, 0, ?)");
    }
    
    @Test
    public void testIndexOnClusteringValueInsertValueOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b int, c blob, PRIMARY KEY (a, b))");
        createIndex("CREATE INDEX ON %s(b)");
        succeedInsertWithIndexedValueOver64k("INSERT INTO %s (a, b, c) VALUES (0, 0, ?)");

    }

    @Test
    public void testIndexOnCollectionKeyInsertCollectionValueOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b map<int, blob>, PRIMARY KEY (a))");
        createIndex("CREATE INDEX ON %s(keys(b))");
        succeedInsertWithIndexedValueOver64k("UPDATE %s SET b[0] = ? WHERE a = 0");
    }

    public void failToInsertWithIndexedValueOver64k(String insertCQL) throws Throwable
    {
        ByteBuffer buf = ByteBuffer.allocate(1024 * 65);
        buf.clear();

        //read more than 64k
        for (int i=0; i<1024 + 1; i++)
            buf.put((byte)0);

        try
        {
            execute(insertCQL, buf);
            fail("Expected statement to fail validation");
        }
        catch (InvalidRequestException e)
        {
            // as expected
        }
    }

    public void succeedInsertWithIndexedValueOver64k(String insertCQL) throws Throwable
    {
        ByteBuffer buf = ByteBuffer.allocate(1024 * 65);
        buf.clear();

        //read more than 64k
        for (int i=0; i<1024 + 1; i++)
            buf.put((byte)0);

        execute(insertCQL, buf);
        flush();
    }
}
