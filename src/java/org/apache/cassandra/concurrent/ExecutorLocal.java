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

package org.apache.cassandra.concurrent;

import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;

public interface ExecutorLocal<T>
{
    ExecutorLocal[] all = { Tracing.instance, ClientWarn.instance };

    /**
     * This is called when scheduling the task, and also before calling {@link ExecutorLocal#set(T)} when running on a
     * executor thread.
     *
     * @return The thread-local value that we want to copy across executor boundaries; may be null if not set.
     */
    T get();

    /**
     * Before a task has been run, this will be called with the value from the thread that scheduled the task, and after
     * the task is finished, the value that was previously retrieved from this thread is restored.
     *
     * @param value Value to use for the executor local state; may be null.
     */
    void set(T value);

    class Value<T>
    {
        public final ExecutorLocal<T> local;
        public final T value;

        public Value(ExecutorLocal<T> local)
        {
            this(local, local.get());
        }

        public Value(ExecutorLocal<T> local, T value)
        {
            this.local = local;
            this.value = value;
        }

        public void set()
        {
            local.set(value);
        }

        public static Value[] forAll()
        {
            Value[] values = new Value[all.length];
            for (int i = 0; i < all.length; i++)
                values[i] = new Value(all[i]);
            return values;
        }

        public static boolean hasValue(Value[] values)
        {
            for (Value value : values)
            {
                if (value.value != null)
                    return true;
            }
            return false;
        }

        public static <T> Value[] replace(Value[] base, Value<T> value)
        {
            Value[] values = new Value[base.length];
            for (int i = 0; i < base.length; i++)
            {
                if (base[i].local == value.local)
                    values[i] = value;
                else
                    values[i] = base[i];
            }
            return values;
        }
    }
}
