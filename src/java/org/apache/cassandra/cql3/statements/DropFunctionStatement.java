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
package org.apache.cassandra.cql3.statements;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.transport.Event;

/**
 * A <code>DROP FUNCTION</code> statement parsed from a CQL query.
 */
public final class DropFunctionStatement extends SchemaAlteringStatement
{
    private final FunctionName functionName;
    private final boolean ifExists;
    private final List<CQL3Type.Raw> argRawTypes;
    private final boolean argsPresent;

    public DropFunctionStatement(FunctionName functionName,
                                 List<CQL3Type.Raw> argRawTypes,
                                 boolean argsPresent,
                                 boolean ifExists)
    {
        this.functionName = functionName;
        this.argRawTypes = argRawTypes;
        this.argsPresent = argsPresent;
        this.ifExists = ifExists;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException
    {
        // TODO CASSANDRA-7557 (function DDL permission)

        state.hasAllKeyspacesAccess(Permission.DROP);
    }

    /**
     * The <code>CqlParser</code> only goes as far as extracting the keyword arguments
     * from these statements, so this method is responsible for processing and
     * validating.
     *
     * @throws org.apache.cassandra.exceptions.InvalidRequestException if arguments are missing or unacceptable
     */
    public void validate(ClientState state) throws RequestValidationException
    {
    }

    public Event.SchemaChange changeEvent()
    {
        return null;
    }

    public boolean announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        List<Function> olds = Functions.find(functionName);

        if (!argsPresent && olds != null && olds.size() > 1)
            throw new InvalidRequestException(String.format("'DROP FUNCTION %s' matches multiple function definitions; " +
                                                            "specify the argument types by issuing a statement like " +
                                                            "'DROP FUNCTION %s (type, type, ...)'. Hint: use cqlsh " +
                                                            "'DESCRIBE FUNCTION %s' command to find all overloads",
                                                            functionName, functionName, functionName));

        List<AbstractType<?>> argTypes = new ArrayList<>(argRawTypes.size());
        for (CQL3Type.Raw rawType : argRawTypes)
        {
            // We have no proper keyspace to give, which means that this will break (NPE currently)
            // for UDT: #7791 is open to fix this
            argTypes.add(rawType.prepare(null).getType());
        }

        Function old;
        if (argsPresent)
        {
            old = Functions.find(functionName, argTypes);
            if (old == null)
            {
                if (ifExists)
                    return false;
                // just build a nicer error message
                StringBuilder sb = new StringBuilder();
                for (CQL3Type.Raw rawType : argRawTypes)
                {
                    if (sb.length() > 0)
                        sb.append(", ");
                    sb.append(rawType);
                }
                throw new InvalidRequestException(String.format("Cannot drop non existing function '%s(%s)'",
                                                                functionName, sb));
            }
        }
        else
        {
            if (olds == null || olds.isEmpty())
            {
                if (ifExists)
                    return false;
                throw new InvalidRequestException(String.format("Cannot drop non existing function '%s'", functionName));
            }
            old = olds.get(0);
        }

        if (old.isNative())
            throw new InvalidRequestException(String.format("Cannot drop function '%s' because it is a " +
                                                            "native (built-in) function", functionName));

        MigrationManager.announceFunctionDrop((UDFunction)old, isLocalOnly);
        return true;
    }
}
