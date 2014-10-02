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

package org.apache.cassandra.serializers;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.*;


public class ListSerializer<T> extends CollectionSerializer<List<T>>
{
    // interning instances
    private static final Map<TypeSerializer<?>, ListSerializer> instances = new HashMap<TypeSerializer<?>, ListSerializer>();

    public final TypeSerializer<T> elements;

    public static synchronized <T> ListSerializer<T> getInstance(TypeSerializer<T> elements)
    {
        ListSerializer<T> t = instances.get(elements);
        if (t == null)
        {
            t = new ListSerializer<T>(elements);
            instances.put(elements, t);
        }
        return t;
    }

    private ListSerializer(TypeSerializer<T> elements)
    {
        this.elements = elements;
    }

    public List<ByteBuffer> serializeValues(List<T> values)
    {
        List<ByteBuffer> buffers = new ArrayList<>(values.size());
        for (T value : values)
            buffers.add(elements.serialize(value));
        return buffers;
    }

    public int getElementCount(List<T> value)
    {
        return value.size();
    }

    public void validateForNativeProtocol(ByteBuffer bytes, int version)
    {
        try
        {
            ByteBuffer input = bytes.duplicate();
            int n = readCollectionSize(input, version);
            for (int i = 0; i < n; i++)
                elements.validate(readValue(input, version));
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a list");
        }
    }

    public List<T> deserializeForNativeProtocol(ByteBuffer bytes, int version)
    {
        try
        {
            ByteBuffer input = bytes.duplicate();
            int n = readCollectionSize(input, version);
            List<T> l = new ArrayList<T>(n);
            for (int i = 0; i < n; i++)
            {
                // We can have nulls in lists that are used for IN values
                ByteBuffer databb = readValue(input, version);
                if (databb != null)
                {
                    elements.validate(databb);
                    l.add(elements.deserialize(databb));
                }
                else
                {
                    l.add(null);
                }
            }
            return l;
        }
        catch (BufferUnderflowException e)
        {
            throw new MarshalException("Not enough bytes to read a list");
        }
    }

    public String toString(List<T> value)
    {
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for (T element : value)
        {
            if (isFirst)
            {
                isFirst = false;
            }
            else
            {
                sb.append("; ");
            }
            sb.append(elements.toString(element));
        }
        return sb.toString();
    }

    public Class<List<T>> getType()
    {
        return (Class) List.class;
    }
}
