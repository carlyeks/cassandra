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

package org.apache.cassandra.service;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.datastax.driver.core.Session;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.utils.Pair;

public class WarningTest extends CQLTester
{
    @Test
    public void unloggedBatchTest() throws Exception
    {
        sessionNet(3);
        {
            final SimpleClient clientV4 = new SimpleClient(nativeAddr.getHostAddress(), nativePort, Server.VERSION_4);
            clientV4.connect(false);

            createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

            StringBuilder batch = new StringBuilder();
            batch.append("BEGIN UNLOGGED BATCH\n");
            batch.append("INSERT INTO ").append(KEYSPACE).append('.').append(currentTable()).append(" (pk, v) VALUES (1, '1');");
            batch.append("INSERT INTO ").append(KEYSPACE).append('.').append(currentTable()).append(" (pk, v) VALUES (2, '2');");
            batch.append("APPLY BATCH;");
            QueryMessage query = new QueryMessage(batch.toString(), QueryOptions.DEFAULT);
            Message.Response resp = clientV4.execute(query);
            assert resp.getWarnings() != null;
            clientV4.close();
        }

        {
            final SimpleClient clientV2 = new SimpleClient(nativeAddr.getHostAddress(), nativePort, Server.VERSION_2);
            clientV2.connect(false);

            createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");

            StringBuilder batch = new StringBuilder();
            batch.append("BEGIN UNLOGGED BATCH\n");
            batch.append("INSERT INTO ").append(KEYSPACE).append('.').append(currentTable()).append(" (pk, v) VALUES (1, '1');");
            batch.append("INSERT INTO ").append(KEYSPACE).append('.').append(currentTable()).append(" (pk, v) VALUES (2, '2');");
            batch.append("APPLY BATCH;");
            QueryMessage query = new QueryMessage(batch.toString(), QueryOptions.DEFAULT);
            Message.Response resp = clientV2.execute(query);
            assert resp.getWarnings() == null;
            clientV2.close();
        }
    }
}
