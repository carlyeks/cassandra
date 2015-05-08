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
package org.apache.cassandra.db;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.ByteBufferUtil;

/*
 * The read response message is sent by the server when reading data
 * this encapsulates the keyspacename and the row that has been read.
 * The keyspace name is needed so that we can use it to create repairs.
 */
public class ReadResponse
{
    public static final IVersionedSerializer<ReadResponse> serializer = new ReadResponseSerializer();

    private final Row row;
    private final ByteBuffer digest;
    private final List<String> warnings;

    public ReadResponse(ByteBuffer digest, List<String> warnings)
    {
        assert digest != null;
        this.digest= digest;
        this.row = null;
        this.warnings = warnings;
    }

    public ReadResponse(ByteBuffer digest)
    {
        this(digest, ClientWarn.getWarnings());
    }

    public ReadResponse(Row row, List<String> warnings)
    {
        assert row != null;
        this.row = row;
        this.digest = null;
        this.warnings = warnings;
    }

    public ReadResponse(Row row)
    {
        this(row, ClientWarn.getWarnings());
    }

    public Row row()
    {
        return row;
    }

    public ByteBuffer digest()
    {
        return digest;
    }

    public boolean isDigestQuery()
    {
        return digest != null;
    }

    public boolean hasWarnings()
    {
        return warnings != null;
    }

    public List<String> getWarnings()
    {
        return warnings;
    }
}

class ReadResponseSerializer implements IVersionedSerializer<ReadResponse>
{
    public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
    {
        out.writeInt(response.isDigestQuery() ? response.digest().remaining() : 0);
        ByteBuffer buffer = response.isDigestQuery() ? response.digest() : ByteBufferUtil.EMPTY_BYTE_BUFFER;
        out.write(buffer);
        out.writeBoolean(response.isDigestQuery());
        if (!response.isDigestQuery())
            Row.serializer.serialize(response.row(), out, version);
        if (response.hasWarnings())
        {
            List<String> warnings = response.getWarnings();
            out.writeInt(warnings.size());
            for (String warning : warnings)
                out.writeUTF(warning);
        }
        else
            out.writeInt(0);
    }

    public ReadResponse deserialize(DataInput in, int version) throws IOException
    {
        byte[] digest = null;
        int digestSize = in.readInt();
        if (digestSize > 0)
        {
            digest = new byte[digestSize];
            in.readFully(digest, 0, digestSize);
        }
        boolean isDigest = in.readBoolean();
        assert isDigest == digestSize > 0;

        Row row = null;
        if (!isDigest)
        {
            // This is coming from a remote host
            row = Row.serializer.deserialize(in, version, ColumnSerializer.Flag.FROM_REMOTE);
        }

        int numWarnings = in.readInt();
        List<String> warnings = null;
        if (numWarnings != 0)
        {
            warnings = new ArrayList<>();
            for (int i = 0; i < numWarnings; i++)
                warnings.add(in.readUTF());
        }

        return isDigest ? new ReadResponse(ByteBuffer.wrap(digest), warnings) : new ReadResponse(row, warnings);
    }

    public long serializedSize(ReadResponse response, int version)
    {
        TypeSizes typeSizes = TypeSizes.NATIVE;
        ByteBuffer buffer = response.isDigestQuery() ? response.digest() : ByteBufferUtil.EMPTY_BYTE_BUFFER;
        int size = typeSizes.sizeof(buffer.remaining());
        size += buffer.remaining();
        size += typeSizes.sizeof(response.isDigestQuery());
        if (!response.isDigestQuery())
            size += Row.serializer.serializedSize(response.row(), version);
        if (response.hasWarnings())
        {
            List<String> warnings = response.getWarnings();
            size += typeSizes.sizeof(warnings.size());
            for (String warning: warnings)
                size += typeSizes.sizeof(warning);
        }
        else
            size += typeSizes.sizeof(0);

        return size;
    }
}
