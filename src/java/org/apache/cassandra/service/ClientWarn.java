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

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.utils.FBUtilities;

public class ClientWarn
{
    private static final String truncated = " (truncated)";
    static ThreadLocal<ClientWarn> warnLocal = new ThreadLocal<>();
    private int length = 0;
    private final List<String> warning = new ArrayList<>();

    private ClientWarn()
    {
    }

    public static void warn(String text)
    {
        ClientWarn warner = warnLocal.get();
        if (warner != null)
        {
            warner.send(text);
        }
    }

    private void send(String text)
    {
        // We have already truncated an entry
        if (length == FBUtilities.MAX_UNSIGNED_SHORT)
            return;
        if (length + text.length() > FBUtilities.MAX_UNSIGNED_SHORT - truncated.length())
        {
            text = text.substring(0, FBUtilities.MAX_UNSIGNED_SHORT - truncated.length() - length) + truncated;
        }

        length += text.length();
        warning.add(text);
    }

    public static void captureWarnings()
    {
        warnLocal.set(new ClientWarn());
    }

    public static List<String> getWarnings()
    {
        ClientWarn warner = warnLocal.get();
        if (warner == null || warner.length == 0)
            return null;
        return warner.warning;
    }

    public static void resetWarnings()
    {
        warnLocal.remove();
    }
}
