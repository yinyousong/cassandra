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
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.transport.Event;

/**
 * A <code>CREATE FUNCTION</code> statement parsed from a CQL query.
 */
public final class CreateFunctionStatement extends SchemaAlteringStatement
{
    private final boolean orReplace;
    private final boolean ifNotExists;
    private final FunctionName functionName;
    private final String language;
    private final String body;
    private final boolean deterministic;

    private final List<ColumnIdentifier> argNames;
    private final List<CQL3Type.Raw> argRawTypes;
    private final CQL3Type.Raw rawReturnType;

    public CreateFunctionStatement(FunctionName functionName,
                                   String language,
                                   String body,
                                   boolean deterministic,
                                   List<ColumnIdentifier> argNames,
                                   List<CQL3Type.Raw> argRawTypes,
                                   CQL3Type.Raw rawReturnType,
                                   boolean orReplace,
                                   boolean ifNotExists)
    {
        this.functionName = functionName;
        this.language = language;
        this.body = body;
        this.deterministic = deterministic;
        this.argNames = argNames;
        this.argRawTypes = argRawTypes;
        this.rawReturnType = rawReturnType;
        this.orReplace = orReplace;
        this.ifNotExists = ifNotExists;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException
    {
        // TODO CASSANDRA-7557 (function DDL permission)

        state.hasAllKeyspacesAccess(Permission.CREATE);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        if (ifNotExists && orReplace)
            throw new InvalidRequestException("Cannot use both 'OR REPLACE' and 'IF NOT EXISTS' directives");
    }

    public Event.SchemaChange changeEvent()
    {
        return null;
    }

    public boolean announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        List<AbstractType<?>> argTypes = new ArrayList<>(argRawTypes.size());
        for (CQL3Type.Raw rawType : argRawTypes)
            // We have no proper keyspace to give, which means that this will break (NPE currently)
            // for UDT: #7791 is open to fix this
            argTypes.add(rawType.prepare(null).getType());

        AbstractType<?> returnType = rawReturnType.prepare(null).getType();

        Function old = Functions.find(functionName, argTypes);
        if (old != null)
        {
            if (ifNotExists)
                return false;
            if (!orReplace)
                throw new InvalidRequestException(String.format("Function %s already exists", old));

            // Means we're replacing the function. We still need to validate that 1) it's not a native function and 2) that the return type
            // matches (or that could break existing code badly)
            if (old.isNative())
                throw new InvalidRequestException(String.format("Cannot replace native function %s", old));
            if (!old.returnType().isValueCompatibleWith(returnType))
                throw new InvalidRequestException(String.format("Cannot replace function %s, the new return type %s is not compatible with the return type %s of existing function",
                                                                functionName, returnType.asCQL3Type(), old.returnType().asCQL3Type()));
        }

        MigrationManager.announceNewFunction(UDFunction.create(functionName, argNames, argTypes, returnType, language, body, deterministic), isLocalOnly);
        return true;
    }
}
