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
import java.util.Map;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.MaterializedViewDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.transport.Event;

import static org.apache.cassandra.thrift.ThriftValidation.validateColumnFamily;

public class AlterTableStatement extends SchemaAlteringStatement
{
    public static enum Type
    {
        ADD, ALTER, DROP, OPTS, RENAME
    }

    public final Type oType;
    public final CQL3Type.Raw validator;
    public final ColumnIdentifier.Raw rawColumnName;
    private final CFPropDefs cfProps;
    private final Map<ColumnIdentifier.Raw, ColumnIdentifier.Raw> renames;
    private final boolean isStatic; // Only for ALTER ADD

    public AlterTableStatement(CFName name,
                               Type type,
                               ColumnIdentifier.Raw columnName,
                               CQL3Type.Raw validator,
                               CFPropDefs cfProps,
                               Map<ColumnIdentifier.Raw, ColumnIdentifier.Raw> renames,
                               boolean isStatic)
    {
        super(name);
        this.oType = type;
        this.rawColumnName = columnName;
        this.validator = validator; // used only for ADD/ALTER commands
        this.cfProps = cfProps;
        this.renames = renames;
        this.isStatic = isStatic;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.ALTER);
    }

    public void validate(ClientState state)
    {
        // validated in announceMigration()
    }

    public boolean announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        if (oType != Type.OPTS && Schema.instance.isMaterializedView(keyspace(), columnFamily()))
            throw new InvalidRequestException("Materialized views can not be directly altered, except for table options");


        CFMetaData meta = validateColumnFamily(keyspace(), columnFamily());
        CFMetaData cfm = meta.copy();

        CQL3Type validator = this.validator == null ? null : this.validator.prepare(keyspace());
        ColumnIdentifier columnName = null;
        ColumnDefinition def = null;
        if (rawColumnName != null)
        {
            columnName = rawColumnName.prepare(cfm);
            def = cfm.getColumnDefinition(columnName);
        }

        List<CFMetaData> materializedViewUpdates = null;
        List<CFMetaData> materializedViewDrops = null;

        switch (oType)
        {
            case ADD:
                assert columnName != null;
                if (cfm.isDense())
                    throw new InvalidRequestException("Cannot add new column to a COMPACT STORAGE table");

                if (isStatic)
                {
                    if (!cfm.isCompound())
                        throw new InvalidRequestException("Static columns are not allowed in COMPACT STORAGE tables");
                    if (cfm.clusteringColumns().isEmpty())
                        throw new InvalidRequestException("Static columns are only useful (and thus allowed) if the table has at least one clustering column");
                }

                if (def != null)
                {
                    switch (def.kind)
                    {
                        case PARTITION_KEY:
                        case CLUSTERING_COLUMN:
                            throw new InvalidRequestException(String.format("Invalid column name %s because it conflicts with a PRIMARY KEY part", columnName));
                        default:
                            throw new InvalidRequestException(String.format("Invalid column name %s because it conflicts with an existing column", columnName));
                    }
                }

                // Cannot re-add a dropped counter column. See #7831.
                if (meta.isCounter() && meta.getDroppedColumns().containsKey(columnName))
                    throw new InvalidRequestException(String.format("Cannot re-add previously dropped counter column %s", columnName));

                AbstractType<?> type = validator.getType();
                if (type.isCollection() && type.isMultiCell())
                {
                    if (!cfm.isCompound())
                        throw new InvalidRequestException("Cannot use non-frozen collections in COMPACT STORAGE tables");
                    if (cfm.isSuper())
                        throw new InvalidRequestException("Cannot use non-frozen collections with super column families");

                    // If there used to be a collection column with the same name (that has been dropped), we could still have
                    // some data using the old type, and so we can't allow adding a collection with the same name unless
                    // the types are compatible (see #6276).
                    CFMetaData.DroppedColumn dropped = cfm.getDroppedColumns().get(columnName);
                    // We could have type == null for old dropped columns, in which case we play it safe and refuse
                    if (dropped != null && (dropped.type == null || (dropped.type instanceof CollectionType && !type.isCompatibleWith(dropped.type))))
                        throw new InvalidRequestException(String.format("Cannot add a collection with the name %s " +
                                                                        "because a collection with the same name and a different type%s has already been used in the past",
                                                                        columnName, dropped.type == null ? "" : " (" + dropped.type.asCQL3Type() + ")"));
                }

                Integer componentIndex = cfm.isCompound() ? cfm.comparator.size() : null;
                cfm.addColumnDefinition(isStatic
                                        ? ColumnDefinition.staticDef(cfm, columnName.bytes, type, componentIndex)
                                        : ColumnDefinition.regularDef(cfm, columnName.bytes, type, componentIndex));

                // Adding a column to a table which has a SELECT * materialized view requires the column to be
                // added to the materialized view as well
                for (MaterializedViewDefinition mv : cfm.getMaterializedViews().values())
                {
                    if (mv.included.isEmpty())
                    {
                        CFMetaData indexCfm = Schema.instance.getCFMetaData(keyspace(), mv.viewName).copy();
                        componentIndex = indexCfm.isCompound() ? indexCfm.comparator.size() : null;
                        indexCfm.addColumnDefinition(isStatic
                                                     ? ColumnDefinition.staticDef(indexCfm, columnName.bytes, type, componentIndex)
                                                     : ColumnDefinition.regularDef(indexCfm, columnName.bytes, type, componentIndex));
                        if (materializedViewUpdates == null)
                            materializedViewUpdates = new ArrayList<>();
                        materializedViewUpdates.add(indexCfm);
                    }
                }
                break;

            case ALTER:
                assert columnName != null;
                if (def == null)
                    throw new InvalidRequestException(String.format("Column %s was not found in table %s", columnName, columnFamily()));

                AbstractType<?> validatorType = validator.getType();
                switch (def.kind)
                {
                    case PARTITION_KEY:
                        if (validatorType instanceof CounterColumnType)
                            throw new InvalidRequestException(String.format("counter type is not supported for PRIMARY KEY part %s", columnName));

                        AbstractType<?> currentType = cfm.getKeyValidatorAsClusteringComparator().subtype(def.position());
                        if (!validatorType.isValueCompatibleWith(currentType))
                            throw new ConfigurationException(String.format("Cannot change %s from type %s to type %s: types are incompatible.",
                                                                           columnName,
                                                                           currentType.asCQL3Type(),
                                                                           validator));
                        break;
                    case CLUSTERING_COLUMN:
                        AbstractType<?> oldType = cfm.comparator.subtype(def.position());
                        // Note that CFMetaData.validateCompatibility already validate the change we're about to do. However, the error message it
                        // sends is a bit cryptic for a CQL3 user, so validating here for a sake of returning a better error message
                        // Do note that we need isCompatibleWith here, not just isValueCompatibleWith.
                        if (!validatorType.isCompatibleWith(oldType))
                            throw new ConfigurationException(String.format("Cannot change %s from type %s to type %s: types are not order-compatible.",
                                                                           columnName,
                                                                           oldType.asCQL3Type(),
                                                                           validator));

                        break;
                    case REGULAR:
                    case STATIC:
                        // Thrift allows to change a column validator so CFMetaData.validateCompatibility will let it slide
                        // if we change to an incompatible type (contrarily to the comparator case). But we don't want to
                        // allow it for CQL3 (see #5882) so validating it explicitly here. We only care about value compatibility
                        // though since we won't compare values (except when there is an index, but that is validated by
                        // ColumnDefinition already).
                        if (!validatorType.isValueCompatibleWith(def.type))
                            throw new ConfigurationException(String.format("Cannot change %s from type %s to type %s: types are incompatible.",
                                                                           columnName,
                                                                           def.type.asCQL3Type(),
                                                                           validator));
                        break;
                }
                // In any case, we update the column definition
                cfm.addOrReplaceColumnDefinition(def.withNewType(validatorType));

                // We have to alter the schema of the materialized view table as well; it doesn't affect the definition however
                for (MaterializedViewDefinition mv : cfm.getMaterializedViews().values())
                {
                    if (!mv.includes(columnName)) continue;
                    // We have to use the pre-adjusted CFM, otherwise we can't resolve the Index
                    CFMetaData indexCfm = Schema.instance.getCFMetaData(keyspace(), mv.viewName).copy();
                    indexCfm.addOrReplaceColumnDefinition(def.withNewType(validatorType));

                    if (materializedViewUpdates == null)
                        materializedViewUpdates = new ArrayList<>();
                    materializedViewUpdates.add(indexCfm);
                }
                break;

            case DROP:
                assert columnName != null;
                if (!cfm.isCQLTable())
                    throw new InvalidRequestException("Cannot drop columns from a non-CQL3 table");
                if (def == null)
                    throw new InvalidRequestException(String.format("Column %s was not found in table %s", columnName, columnFamily()));

                switch (def.kind)
                {
                    case PARTITION_KEY:
                    case CLUSTERING_COLUMN:
                        throw new InvalidRequestException(String.format("Cannot drop PRIMARY KEY part %s", columnName));
                    case REGULAR:
                    case STATIC:
                        ColumnDefinition toDelete = null;
                        for (ColumnDefinition columnDef : cfm.partitionColumns())
                        {
                            if (columnDef.name.equals(columnName))
                            {
                                toDelete = columnDef;
                                break;
                            }
                        }
                        assert toDelete != null;
                        cfm.removeColumnDefinition(toDelete);
                        cfm.recordColumnDrop(toDelete);
                        break;
                }

                // If a column is dropped which is the target of a materialized view,
                // then we need to drop the view.
                // If a column is dropped which was selected into a materialized view,
                // we need to drop that column from the included materialzied view table
                // and definition.
                boolean rejectAlter = false;
                StringBuilder builder = new StringBuilder();
                for (MaterializedViewDefinition mv : cfm.getMaterializedViews().values())
                {
                    if (!mv.includes(columnName)) continue;
                    if (rejectAlter)
                        builder.append(',');
                    rejectAlter = true;
                    builder.append(mv.viewName);
                }
                if (rejectAlter)
                    throw new InvalidRequestException(String.format("Cannot drop column %s, depended on by materialized views (%s.{%s})",
                                                                    columnName.toString(),
                                                                    keyspace(),
                                                                    builder.toString()));
                break;
            case OPTS:
                if (cfProps == null)
                    throw new InvalidRequestException("ALTER TABLE WITH invoked, but no parameters found");

                cfProps.validate();

                if (meta.isCounter() && cfProps.getDefaultTimeToLive() > 0)
                    throw new InvalidRequestException("Cannot set default_time_to_live on a table with counters");

                cfProps.applyToCFMetadata(cfm);
                break;
            case RENAME:
                for (Map.Entry<ColumnIdentifier.Raw, ColumnIdentifier.Raw> entry : renames.entrySet())
                {
                    ColumnIdentifier from = entry.getKey().prepare(cfm);
                    ColumnIdentifier to = entry.getValue().prepare(cfm);
                    cfm.renameColumn(from, to);

                    // If the materialized view includes a renamed column, it must be renamed in the index table and the definition.
                    for (MaterializedViewDefinition mv : cfm.getMaterializedViews().values())
                    {
                        if (!mv.includes(from)) continue;

                        CFMetaData indexCfm = Schema.instance.getCFMetaData(keyspace(), mv.viewName).copy();
                        ColumnIdentifier indexFrom = entry.getKey().prepare(indexCfm);
                        ColumnIdentifier indexTo = entry.getValue().prepare(indexCfm);
                        indexCfm.renameColumn(indexFrom, indexTo);

                        MaterializedViewDefinition giCopy = mv.copy();
                        mv.renameColumn(from, to);

                        cfm.replaceMaterializedView(mv);

                        if (materializedViewUpdates == null)
                            materializedViewUpdates = new ArrayList<>();
                        materializedViewUpdates.add(indexCfm);
                    }
                }
        }

        if (materializedViewUpdates != null)
        {
            for (CFMetaData mvUpdates : materializedViewUpdates)
                MigrationManager.announceColumnFamilyUpdate(mvUpdates, false, isLocalOnly);
        }
        if (materializedViewDrops != null)
        {
            for (CFMetaData mvDrops : materializedViewDrops)
                MigrationManager.announceColumnFamilyDrop(mvDrops.ksName, mvDrops.cfName, isLocalOnly);
        }

        MigrationManager.announceColumnFamilyUpdate(cfm, false, isLocalOnly);
        return true;
    }

    public String toString()
    {
        return String.format("AlterTableStatement(name=%s, type=%s, column=%s, validator=%s)",
                             cfName,
                             oType,
                             rawColumnName,
                             validator);
    }

    public Event.SchemaChange changeEvent()
    {
        return new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily());
    }
}
