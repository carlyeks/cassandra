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

package org.apache.cassandra.cql3;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import junit.framework.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.serializers.TimeSerializer;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.utils.ByteBufferUtil;

public class MaterializedViewTest extends CQLTester
{
    int protocolVersion = 3;

    @BeforeClass
    public static void startup()
    {
        requireNetwork();
    }

    @Test
    public void testAllTypes() throws Throwable
    {
        String myType = createType("CREATE TYPE %s (a int, b uuid, c set<text>)");

        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "blobval blob, " +
                    "booleanval boolean, " +
                    "dateval date, " +
                    "decimalval decimal, " +
                    "doubleval double, " +
                    "floatval float, " +
                    "inetval inet, " +
                    "intval int, " +
                    "textval text, " +
                    "timeval time, " +
                    "timestampval timestamp, " +
                    "timeuuidval timeuuid, " +
                    "uuidval uuid," +
                    "varcharval varchar, " +
                    "varintval varint, " +
                    "listval list<int>, " +
                    "frozenlistval frozen<list<int>>, " +
                    "setval set<uuid>, " +
                    "frozensetval frozen<set<uuid>>, " +
                    "mapval map<ascii, int>," +
                    "frozenmapval frozen<map<ascii, int>>," +
                    "tupleval frozen<tuple<int, ascii, uuid>>," +
                    "udtval frozen<" + myType + ">)");


        CFMetaData metadata = currentTableMetadata();

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        for (ColumnDefinition def : new HashSet<>(metadata.allColumns()))
        {
            if (def.isPrimaryKeyColumn())
                continue;

            if (!def.type.isMultiCell())
                executeNet(protocolVersion, "CREATE MATERIALIZED VIEW "+keyspace()+".mv_"+def.name+" AS SELECT * FROM %s PRIMARY KEY ("+def.name+",k)");
        }

        // fromJson() can only be used when the receiver type is known
        assertInvalidMessage("fromJson() cannot be used in the selection clause", "SELECT fromJson(asciival) FROM %s", 0, 0);

        String func1 = createFunction(KEYSPACE, "int", "CREATE FUNCTION %s (a int) CALLED ON NULL INPUT RETURNS text LANGUAGE java AS $$ return a.toString(); $$");
        createFunctionOverload(func1, "int", "CREATE FUNCTION %s (a text) CALLED ON NULL INPUT RETURNS text LANGUAGE java AS $$ return new String(a); $$");

        // ================ ascii ================
        executeNet(protocolVersion, "INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii text\"");
        assertRows(execute("SELECT k, asciival FROM %s WHERE k = ?", 0), row(0, "ascii text"));

        executeNet(protocolVersion, "INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii \\\" text\"");
        assertRows(execute("SELECT k, asciival FROM %s WHERE k = ?", 0), row(0, "ascii \" text"));

        // test that we can use fromJson() in other valid places in queries
        assertRows(execute("SELECT asciival FROM %s WHERE k = fromJson(?)", "0"), row("ascii \" text"));


        //Check the MV
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii text"));
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii \" text"), row(0, null));

        executeNet(protocolVersion, "UPDATE %s SET asciival = fromJson(?) WHERE k = fromJson(?)", "\"ascii \\\" text\"", "0");
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii \" text"), row(0, null));

        executeNet(protocolVersion, "DELETE FROM %s WHERE k = fromJson(?)", "0");
        assertRows(execute("SELECT k, asciival FROM %s WHERE k = ?", 0));
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii \" text"));


        executeNet(protocolVersion, "INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii text\"");
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii text"), row(0, null));


        // ================ bigint ================
        executeNet(protocolVersion, "INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "123123123123");
        assertRows(execute("SELECT k, bigintval FROM %s WHERE k = ?", 0), row(0, 123123123123L));

        assertRows(execute("SELECT k, asciival from mv_bigintval WHERE bigintval = ?", 123123123123L), row(0, "ascii text"));


        // ================ blob ================
        executeNet(protocolVersion, "INSERT INTO %s (k, blobval) VALUES (?, fromJson(?))", 0, "\"0x00000001\"");
        assertRows(execute("SELECT k, blobval FROM %s WHERE k = ?", 0), row(0, ByteBufferUtil.bytes(1)));

        assertRows(execute("SELECT k, asciival from mv_blobval WHERE blobval = ?", ByteBufferUtil.bytes(1)), row(0, "ascii text"));

        // ================ boolean ================
        executeNet(protocolVersion, "INSERT INTO %s (k, booleanval) VALUES (?, fromJson(?))", 0, "true");
        assertRows(execute("SELECT k, booleanval FROM %s WHERE k = ?", 0), row(0, true));

        assertRows(execute("SELECT k, asciival from mv_booleanval WHERE booleanval = ?", true), row(0, "ascii text"));

        executeNet(protocolVersion, "INSERT INTO %s (k, booleanval) VALUES (?, fromJson(?))", 0, "false");
        assertRows(execute("SELECT k, booleanval FROM %s WHERE k = ?", 0), row(0, false));

        assertRows(execute("SELECT k, asciival from mv_booleanval WHERE booleanval = ?", true));
        assertRows(execute("SELECT k, asciival from mv_booleanval WHERE booleanval = ?", false), row(0, "ascii text"));

        // ================ date ================
        executeNet(protocolVersion, "INSERT INTO %s (k, dateval) VALUES (?, fromJson(?))", 0, "\"1987-03-23\"");
        assertRows(execute("SELECT k, dateval FROM %s WHERE k = ?", 0), row(0, SimpleDateSerializer.dateStringToDays("1987-03-23")));

        assertRows(execute("SELECT k, asciival from mv_dateval WHERE dateval = fromJson(?)", "\"1987-03-23\""), row(0, "ascii text"));

        // ================ decimal ================
        executeNet(protocolVersion, "INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "123123.123123");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123.123123")));

        assertRows(execute("SELECT k, asciival from mv_decimalval WHERE decimalval = fromJson(?)", "123123.123123"), row(0, "ascii text"));


        executeNet(protocolVersion, "INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123")));

        assertRows(execute("SELECT k, asciival from mv_decimalval WHERE decimalval = fromJson(?)", "123123.123123"));
        assertRows(execute("SELECT k, asciival from mv_decimalval WHERE decimalval = fromJson(?)", "123123"), row(0, "ascii text"));


        // accept strings for numbers that cannot be represented as doubles
        executeNet(protocolVersion, "INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "\"123123.123123\"");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123.123123")));

        assertRows(execute("SELECT k, asciival from mv_decimalval WHERE decimalval = fromJson(?)", "\"123123.123123\""), row(0, "ascii text"));


        executeNet(protocolVersion, "INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "\"-1.23E-12\"");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("-1.23E-12")));

        assertRows(execute("SELECT k, asciival from mv_decimalval WHERE decimalval = fromJson(?)", "\"-1.23E-12\""), row(0, "ascii text"));


        // ================ double ================
        executeNet(protocolVersion, "INSERT INTO %s (k, doubleval) VALUES (?, fromJson(?))", 0, "123123.123123");
        assertRows(execute("SELECT k, doubleval FROM %s WHERE k = ?", 0), row(0, 123123.123123d));

        assertRows(execute("SELECT k, asciival from mv_doubleval WHERE doubleval = fromJson(?)", "123123.123123"), row(0, "ascii text"));


        executeNet(protocolVersion, "INSERT INTO %s (k, doubleval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, doubleval FROM %s WHERE k = ?", 0), row(0, 123123.0d));

        assertRows(execute("SELECT k, asciival from mv_doubleval WHERE doubleval = fromJson(?)", "123123"), row(0, "ascii text"));

        // ================ float ================
        executeNet(protocolVersion, "INSERT INTO %s (k, floatval) VALUES (?, fromJson(?))", 0, "123123.123123");
        assertRows(execute("SELECT k, floatval FROM %s WHERE k = ?", 0), row(0, 123123.123123f));

        assertRows(execute("SELECT k, asciival from mv_floatval WHERE floatval = fromJson(?)", "123123.123123"), row(0, "ascii text"));


        executeNet(protocolVersion, "INSERT INTO %s (k, floatval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, floatval FROM %s WHERE k = ?", 0), row(0, 123123.0f));

        assertRows(execute("SELECT k, asciival from mv_floatval WHERE floatval = fromJson(?)", "123123"), row(0, "ascii text"));


        // ================ inet ================
        executeNet(protocolVersion, "INSERT INTO %s (k, inetval) VALUES (?, fromJson(?))", 0, "\"127.0.0.1\"");
        assertRows(execute("SELECT k, inetval FROM %s WHERE k = ?", 0), row(0, InetAddress.getByName("127.0.0.1")));

        assertRows(execute("SELECT k, asciival from mv_inetval WHERE inetval = fromJson(?)", "\"127.0.0.1\""), row(0, "ascii text"));

        executeNet(protocolVersion, "INSERT INTO %s (k, inetval) VALUES (?, fromJson(?))", 0, "\"::1\"");
        assertRows(execute("SELECT k, inetval FROM %s WHERE k = ?", 0), row(0, InetAddress.getByName("::1")));

        assertRows(execute("SELECT k, asciival from mv_inetval WHERE inetval = fromJson(?)", "\"127.0.0.1\""));
        assertRows(execute("SELECT k, asciival from mv_inetval WHERE inetval = fromJson(?)", "\"::1\""), row(0, "ascii text"));

        // ================ int ================
        executeNet(protocolVersion, "INSERT INTO %s (k, intval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 123123));

        assertRows(execute("SELECT k, asciival from mv_intval WHERE intval = fromJson(?)", "123123"), row(0, "ascii text"));


        // ================ text (varchar) ================
        executeNet(protocolVersion, "INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"some \\\" text\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "some \" text"));

        executeNet(protocolVersion, "INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"\\u2013\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "\u2013"));

        assertRows(execute("SELECT k, asciival from mv_textval WHERE textval = fromJson(?)", "\"\\u2013\""), row(0, "ascii text"));

        executeNet(protocolVersion, "INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"abcd\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "abcd"));

        assertRows(execute("SELECT k, asciival from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, "ascii text"));


        // ================ time ================
        executeNet(protocolVersion, "INSERT INTO %s (k, timeval) VALUES (?, fromJson(?))", 0, "\"07:35:07.000111222\"");
        assertRows(execute("SELECT k, timeval FROM %s WHERE k = ?", 0), row(0, TimeSerializer.timeStringToLong("07:35:07.000111222")));

        assertRows(execute("SELECT k, asciival from mv_timeval WHERE timeval = fromJson(?)", "\"07:35:07.000111222\""), row(0, "ascii text"));

        // ================ timestamp ================
        executeNet(protocolVersion, "INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "123123123123");
        assertRows(execute("SELECT k, timestampval FROM %s WHERE k = ?", 0), row(0, new Date(123123123123L)));

        assertRows(execute("SELECT k, asciival from mv_timestampval WHERE timestampval = fromJson(?)", "123123123123"), row(0, "ascii text"));


        executeNet(protocolVersion, "INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "\"2014-01-01\"");
        assertRows(execute("SELECT k, timestampval FROM %s WHERE k = ?", 0), row(0, new SimpleDateFormat("y-M-d").parse("2014-01-01")));

        assertRows(execute("SELECT k, asciival from mv_timestampval WHERE timestampval = fromJson(?)", "\"2014-01-01\""), row(0, "ascii text"));


        // ================ timeuuid ================
        executeNet(protocolVersion, "INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\"");
        assertRows(execute("SELECT k, timeuuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        executeNet(protocolVersion, "INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\"");
        assertRows(execute("SELECT k, timeuuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));


        assertRows(execute("SELECT k, asciival from mv_timeuuidval WHERE timeuuidval = fromJson(?)", "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\""), row(0, "ascii text"));

        // ================ uuidval ================
        executeNet(protocolVersion, "INSERT INTO %s (k, uuidval) VALUES (?, fromJson(?))", 0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\"");
        assertRows(execute("SELECT k, uuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        executeNet(protocolVersion, "INSERT INTO %s (k, uuidval) VALUES (?, fromJson(?))", 0, "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\"");
        assertRows(execute("SELECT k, uuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        assertRows(execute("SELECT k, asciival from mv_uuidval WHERE uuidval = fromJson(?)", "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\""), row(0, "ascii text"));


        // ================ varint ================
        executeNet(protocolVersion, "INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "123123123123");
        assertRows(execute("SELECT k, varintval FROM %s WHERE k = ?", 0), row(0, new BigInteger("123123123123")));

        assertRows(execute("SELECT k, asciival from mv_varintval WHERE varintval = fromJson(?)", "123123123123"), row(0, "ascii text"));


        // accept strings for numbers that cannot be represented as longs
        executeNet(protocolVersion, "INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "\"1234567890123456789012345678901234567890\"");
        assertRows(execute("SELECT k, varintval FROM %s WHERE k = ?", 0), row(0, new BigInteger("1234567890123456789012345678901234567890")));

        assertRows(execute("SELECT k, asciival from mv_varintval WHERE varintval = fromJson(?)", "\"1234567890123456789012345678901234567890\""), row(0, "ascii text"));


        // ================ lists ================
        executeNet(protocolVersion, "INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[1, 2, 3]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(1, 2, 3)));

        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(1, 2, 3)));

        executeNet(protocolVersion, "INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[1]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(1)));

        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(1)));

        executeNet(protocolVersion, "UPDATE %s SET listval = listval + fromJson(?) WHERE k = ?", "[2]", 0);
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(1, 2)));

        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(1, 2)));

        executeNet(protocolVersion, "UPDATE %s SET listval = fromJson(?) + listval WHERE k = ?", "[0]", 0);
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(0, 1, 2)));

        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(0, 1, 2)));

        executeNet(protocolVersion, "UPDATE %s SET listval[1] = fromJson(?) WHERE k = ?", "10", 0);
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(0, 10, 2)));

        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(0, 10, 2)));

        executeNet(protocolVersion, "DELETE listval[1] FROM %s WHERE k = ?", 0);
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(0, 2)));

        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(0, 2)));


        executeNet(protocolVersion, "INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, null));

        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, null));

        // frozen
        executeNet(protocolVersion, "INSERT INTO %s (k, frozenlistval) VALUES (?, fromJson(?))", 0, "[1, 2, 3]");
        assertRows(execute("SELECT k, frozenlistval FROM %s WHERE k = ?", 0), row(0, list(1, 2, 3)));

        assertRows(execute("SELECT k, frozenlistval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(1, 2, 3)));
        assertRows(execute("SELECT k, textval from mv_frozenlistval where frozenlistval = fromJson(?)", "[1, 2, 3]"), row(0, "abcd"));

        executeNet(protocolVersion, "INSERT INTO %s (k, frozenlistval) VALUES (?, fromJson(?))", 0, "[3, 2, 1]");
        assertRows(execute("SELECT k, frozenlistval FROM %s WHERE k = ?", 0), row(0, list(3, 2, 1)));
        assertRows(execute("SELECT k, textval from mv_frozenlistval where frozenlistval = fromJson(?)", "[1, 2, 3]"));
        assertRows(execute("SELECT k, textval from mv_frozenlistval where frozenlistval = fromJson(?)", "[3, 2, 1]"), row(0, "abcd"));


        assertRows(execute("SELECT k, frozenlistval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(3, 2, 1)));

        executeNet(protocolVersion, "INSERT INTO %s (k, frozenlistval) VALUES (?, fromJson(?))", 0, "[]");
        assertRows(execute("SELECT k, frozenlistval FROM %s WHERE k = ?", 0), row(0, list()));

        assertRows(execute("SELECT k, frozenlistval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list()));


        // ================ sets ================
        executeNet(protocolVersion, "INSERT INTO %s (k, setval) VALUES (?, fromJson(?))",
                   0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );

        assertRows(execute("SELECT k, setval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))));


        // duplicates are okay, just like in CQL
        executeNet(protocolVersion, "INSERT INTO %s (k, setval) VALUES (?, fromJson(?))",
                   0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );

        assertRows(execute("SELECT k, setval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))));

        executeNet(protocolVersion, "UPDATE %s SET setval = setval + fromJson(?) WHERE k = ?", "[\"6bddc89a-5644-0000-97fc-56847afe9799\"]", 0);

        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-0000-97fc-56847afe9799"), UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );

        assertRows(execute("SELECT k, setval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-5644-0000-97fc-56847afe9799"), UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))));



        executeNet(protocolVersion, "UPDATE %s SET setval = setval - fromJson(?) WHERE k = ?", "[\"6bddc89a-5644-0000-97fc-56847afe9799\"]", 0);

        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );

        assertRows(execute("SELECT k, setval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))));


        executeNet(protocolVersion, "INSERT INTO %s (k, setval) VALUES (?, fromJson(?))", 0, "[]");
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0), row(0, null));

        assertRows(execute("SELECT k, setval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, null));


        // frozen
        executeNet(protocolVersion, "INSERT INTO %s (k, frozensetval) VALUES (?, fromJson(?))",
                   0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, frozensetval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );

        assertRows(execute("SELECT k, frozensetval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))));

        executeNet(protocolVersion, "INSERT INTO %s (k, frozensetval) VALUES (?, fromJson(?))",
                   0, "[\"6bddc89a-0000-11e4-97fc-56847afe9799\", \"6bddc89a-5644-11e4-97fc-56847afe9798\"]");
        assertRows(execute("SELECT k, frozensetval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-0000-11e4-97fc-56847afe9799"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"))))
        );

        assertRows(execute("SELECT k, frozensetval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-0000-11e4-97fc-56847afe9799"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798")))));


        // ================ maps ================
        executeNet(protocolVersion, "INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": 2}");
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0), row(0, map("a", 1, "b", 2)));

        assertRows(execute("SELECT k, mapval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, map("a", 1, "b", 2)));

        executeNet(protocolVersion, "UPDATE %s SET mapval[?] = ?  WHERE k = ?", "c", 3, 0);

        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0),
                   row(0, map("a", 1, "b", 2, "c", 3))
        );

        assertRows(execute("SELECT k, mapval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, map("a", 1, "b", 2, "c", 3)));


        executeNet(protocolVersion, "UPDATE %s SET mapval[?] = ?  WHERE k = ?", "b", 10, 0);

        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0),
                   row(0, map("a", 1, "b", 10, "c", 3))
        );

        assertRows(execute("SELECT k, mapval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, map("a", 1, "b", 10, "c", 3)));


        executeNet(protocolVersion, "DELETE mapval[?] FROM %s WHERE k = ?", "b", 0);

        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0),
                   row(0, map("a", 1, "c", 3))
        );

        assertRows(execute("SELECT k, mapval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, map("a", 1, "c", 3)));

        executeNet(protocolVersion, "INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{}");
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0), row(0, null));


        assertRows(execute("SELECT k, mapval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, null));

        // frozen
        executeNet(protocolVersion, "INSERT INTO %s (k, frozenmapval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": 2}");
        assertRows(execute("SELECT k, frozenmapval FROM %s WHERE k = ?", 0), row(0, map("a", 1, "b", 2)));

        assertRows(execute("SELECT k, textval FROM mv_frozenmapval WHERE frozenmapval = fromJson(?)", "{\"a\": 1, \"b\": 2}"), row(0, "abcd"));


        executeNet(protocolVersion, "INSERT INTO %s (k, frozenmapval) VALUES (?, fromJson(?))", 0, "{\"b\": 2, \"a\": 3}");
        assertRows(execute("SELECT k, frozenmapval FROM %s WHERE k = ?", 0), row(0, map("a", 3, "b", 2)));

        assertRows(execute("SELECT k, frozenmapval FROM %s WHERE k = ?", 0), row(0, map("a", 3, "b", 2)));


        // ================ tuples ================
        executeNet(protocolVersion, "INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))", 0, "[1, \"foobar\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, tupleval FROM %s WHERE k = ?", 0),
                   row(0, tuple(1, "foobar", UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))
        );

        assertRows(execute("SELECT k, textval FROM mv_tupleval WHERE tupleval = ?", tuple(1, "foobar", UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))),
                   row(0, "abcd"));


        executeNet(protocolVersion, "INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))", 0, "[1, null, \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, tupleval FROM %s WHERE k = ?", 0),
                   row(0, tuple(1, null, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))
        );

        assertRows(execute("SELECT k, textval FROM mv_tupleval WHERE tupleval = ?", tuple(1, "foobar", UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))));

        assertRows(execute("SELECT k, textval FROM mv_tupleval WHERE tupleval = ?", tuple(1, null, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))),
                   row(0, "abcd"));


        // ================ UDTs ================
        executeNet(protocolVersion, "INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                   row(0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("bar", "foo"))
        );

        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = fromJson(?)", "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"),
                   row(0, "abcd"));


        // order of fields shouldn't matter
        executeNet(protocolVersion, "INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"a\": 1, \"c\": [\"foo\", \"bar\"]}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                   row(0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("bar", "foo"))
        );

        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = fromJson(?)", "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"),
                   row(0, "abcd"));


        // test nulls
        executeNet(protocolVersion, "INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": null, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                   row(0, null, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("bar", "foo"))
        );

        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = fromJson(?)", "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"));

        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = fromJson(?)", "{\"a\": null, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"),
                   row(0, "abcd"));


        // test missing fields
        executeNet(protocolVersion, "INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\"}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                   row(0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), null)
        );

        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = fromJson(?)", "{\"a\": null, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"));

        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = fromJson(?)", "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\"}"),
                   row(0, "abcd"));
        
    }
}
