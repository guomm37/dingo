/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.calcite.executor;

import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.grammar.ddl.SqlAdminRollback;
import io.dingodb.calcite.grammar.ddl.SqlAnalyze;
import io.dingodb.calcite.grammar.ddl.SqlBeginTx;
import io.dingodb.calcite.grammar.ddl.SqlCall;
import io.dingodb.calcite.grammar.ddl.SqlCommit;
import io.dingodb.calcite.grammar.ddl.SqlKillConnection;
import io.dingodb.calcite.grammar.ddl.SqlKillQuery;
import io.dingodb.calcite.grammar.ddl.SqlLoadData;
import io.dingodb.calcite.grammar.ddl.SqlLockBlock;
import io.dingodb.calcite.grammar.ddl.SqlLockTable;
import io.dingodb.calcite.grammar.ddl.SqlRollback;
import io.dingodb.calcite.grammar.ddl.SqlUnLockBlock;
import io.dingodb.calcite.grammar.ddl.SqlUnLockTable;
import io.dingodb.calcite.grammar.dql.SqlBackUpTimePoint;
import io.dingodb.calcite.grammar.dql.SqlBackUpTsoPoint;
import io.dingodb.calcite.grammar.dql.SqlDescTable;
import io.dingodb.calcite.grammar.dql.SqlNextAutoIncrement;
import io.dingodb.calcite.grammar.dql.SqlShowCharset;
import io.dingodb.calcite.grammar.dql.SqlShowCollation;
import io.dingodb.calcite.grammar.dql.SqlShowColumns;
import io.dingodb.calcite.grammar.dql.SqlShowCreateTable;
import io.dingodb.calcite.grammar.dql.SqlShowCreateUser;
import io.dingodb.calcite.grammar.dql.SqlShowDatabases;
import io.dingodb.calcite.grammar.dql.SqlShowEngines;
import io.dingodb.calcite.grammar.dql.SqlShowExecutors;
import io.dingodb.calcite.grammar.dql.SqlShowFullTables;
import io.dingodb.calcite.grammar.dql.SqlShowGrants;
import io.dingodb.calcite.grammar.dql.SqlShowIndexFromTable;
import io.dingodb.calcite.grammar.dql.SqlShowLocks;
import io.dingodb.calcite.grammar.dql.SqlShowPlugins;
import io.dingodb.calcite.grammar.dql.SqlShowProcessList;
import io.dingodb.calcite.grammar.dql.SqlShowStartTs;
import io.dingodb.calcite.grammar.dql.SqlShowStatus;
import io.dingodb.calcite.grammar.dql.SqlShowTableDistribution;
import io.dingodb.calcite.grammar.dql.SqlShowTableIndex;
import io.dingodb.calcite.grammar.dql.SqlShowTableStatus;
import io.dingodb.calcite.grammar.dql.SqlShowTables;
import io.dingodb.calcite.grammar.dql.SqlShowTenants;
import io.dingodb.calcite.grammar.dql.SqlShowTriggers;
import io.dingodb.calcite.grammar.dql.SqlShowVariables;
import io.dingodb.calcite.grammar.dql.SqlShowWarnings;
import io.dingodb.calcite.grammar.dql.SqlStartGc;
import io.dingodb.exec.transaction.base.TransactionType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

public final class SqlToExecutorConverter {

    private SqlToExecutorConverter() {
    }

    public static Optional<Executor> convert(SqlNode sqlNode, Connection connection, DingoParserContext context) {
        if (sqlNode instanceof SqlShowWarnings) {
            return Optional.of(new ShowWarningsExecutor(context));
        } else if (sqlNode instanceof SqlShowGrants) {
            return Optional.of(new ShowGrantsExecutor(sqlNode, connection));
        } else if (sqlNode instanceof SqlShowDatabases) {
            SqlShowDatabases sqlShowDatabases = (SqlShowDatabases) sqlNode;
            return Optional.of(new ShowDatabaseExecutor(connection, sqlShowDatabases.sqlLikePattern));
        } else if (sqlNode instanceof SqlShowTables) {
            String usedSchema = "";
            if (context.getUsedSchema() != null) {
                usedSchema = context.getUsedSchema().getName();
            }
            SqlShowTables sqlShowTables = (SqlShowTables) sqlNode;
            String pattern = sqlShowTables.sqlLikePattern;
            return Optional.of(new ShowTableExecutor(usedSchema, connection, pattern));
        } else if (sqlNode instanceof SqlShowFullTables) {
            SqlShowFullTables showFullTables = (SqlShowFullTables) sqlNode;
            return Optional.of(
                new ShowFullTableExecutor(
                    showFullTables.schema,
                    showFullTables.pattern,
                    connection,
                    showFullTables.condition)
            );
        } else if (sqlNode instanceof SqlNextAutoIncrement) {
            SqlNextAutoIncrement sqlNextAutoIncrement = (SqlNextAutoIncrement) sqlNode;
            if (StringUtils.isEmpty(sqlNextAutoIncrement.schemaName)) {
                sqlNextAutoIncrement.schemaName = getSchemaName(context);
            }
            return Optional.of(new ShowNextAutoIncrementExecutor(sqlNextAutoIncrement));
        } else if (sqlNode instanceof SqlShowVariables) {
            SqlShowVariables sqlShowVariables = (SqlShowVariables) sqlNode;
            return Optional.of(new ShowVariablesExecutor(sqlShowVariables.sqlLikePattern, sqlShowVariables.isGlobal,
                connection));
        } else if (sqlNode instanceof SqlSetOption) {
            SqlSetOption setOption = (SqlSetOption) sqlNode;
            return Optional.of(new SetOptionExecutor(connection, setOption));
        } else if (sqlNode instanceof SqlShowCreateTable) {
            return Optional.of(new ShowCreateTableExecutor(sqlNode, getSchemaName(context)));
        } else if (sqlNode instanceof SqlShowCreateUser) {
            SqlShowCreateUser sqlShowCreateUser = (SqlShowCreateUser) sqlNode;
            return Optional.of(new ShowCreateUserExecutor(sqlNode,
                sqlShowCreateUser.userName,
                sqlShowCreateUser.host));
        } else if (sqlNode instanceof SqlShowColumns) {
            SqlShowColumns showColumns = (SqlShowColumns) sqlNode;
            if (StringUtils.isEmpty(showColumns.schemaName)) {
                showColumns.schemaName = getSchemaName(context);
            }
            return Optional.of(new ShowColumnsExecutor(sqlNode));
        } else if (sqlNode instanceof SqlShowTableDistribution) {
            SqlShowTableDistribution sqlShowTableDistribution = (SqlShowTableDistribution) sqlNode;
            if (sqlShowTableDistribution.schemaName == null) {
                sqlShowTableDistribution.schemaName = getSchemaName(context);
            }
            return Optional.of(new ShowTableDistributionExecutor(
                sqlNode, sqlShowTableDistribution.schemaName, sqlShowTableDistribution.tableName
            ));
        } else if (sqlNode instanceof SqlDescTable) {
            SqlDescTable sqlDesc = (SqlDescTable) sqlNode;
            if (StringUtils.isEmpty(sqlDesc.schemaName)) {
                sqlDesc.schemaName = getSchemaName(context);
            }
            SqlShowColumns sqlShowColumns = new SqlShowColumns(sqlDesc.pos, sqlDesc.schemaName, sqlDesc.tableName, "");
            return Optional.of(new ShowColumnsExecutor(sqlShowColumns));
        } else if (sqlNode instanceof SqlShowTableStatus) {
            SqlShowTableStatus showTableStatus = (SqlShowTableStatus) sqlNode;
            if (StringUtils.isEmpty(showTableStatus.schemaName)) {
                showTableStatus.schemaName = getSchemaName(context);
            }
            return Optional.of(new ShowTableStatusExecutor(showTableStatus.schemaName, showTableStatus.sqlLikePattern));
        } else if (sqlNode instanceof SqlAnalyze) {
            SqlAnalyze analyze = (SqlAnalyze) sqlNode;
            if (StringUtils.isEmpty(analyze.getSchemaName())) {
                analyze.setSchemaName(getSchemaName(context));
            }
            return Optional.of(new AnalyzeTableExecutor(analyze, connection));
        } else if (sqlNode instanceof SqlShowTableIndex) {
            SqlShowTableIndex sqlShowTableIndex = (SqlShowTableIndex) sqlNode;
            return Optional.of(new ShowTableIndexExecutor(sqlNode, sqlShowTableIndex.tableName));
        } else if (sqlNode instanceof SqlCommit) {
            return Optional.of(new CommitTxExecutor(connection));
        } else if (sqlNode instanceof SqlRollback) {
            return Optional.of(new RollbackTxExecutor(connection));
        } else if (sqlNode instanceof SqlBeginTx) {
            SqlBeginTx sqlBeginTx = (SqlBeginTx) sqlNode;
            boolean pessimistic = false;
            try {
                if (TransactionType.PESSIMISTIC.name().equalsIgnoreCase(sqlBeginTx.txnMode)) {
                    pessimistic = true;
                } else if (sqlBeginTx.txnMode.isEmpty()
                    && TransactionType.PESSIMISTIC.name().equalsIgnoreCase(connection.getClientInfo("txn_mode"))) {
                    pessimistic = true;
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return Optional.of(new StartTransactionExecutor(connection, pessimistic));
        } else if (sqlNode instanceof SqlLockTable) {
            SqlLockTable sqlLockTable = (SqlLockTable) sqlNode;
            String usedSchemaName = getSchemaName(context);
            return Optional.of(new LockTableExecutor(connection, sqlLockTable.tableList, usedSchemaName));
        } else if (sqlNode instanceof SqlLockBlock) {
            SqlLockBlock sqlLockBlock = (SqlLockBlock) sqlNode;
            return Optional.of(new LockBlockExecutor(connection, sqlLockBlock.getSqlBlockList()));
        } else if (sqlNode instanceof SqlUnLockTable) {
            return Optional.of(new UnlockTableExecutor(connection));
        } else if (sqlNode instanceof SqlUnLockBlock) {
            return Optional.of(new UnlockBlockExecutor(connection));
        } else if (sqlNode instanceof SqlKillQuery) {
            SqlKillQuery killQuery = (SqlKillQuery) sqlNode;
            return Optional.of(new KillQuery(killQuery.getThreadId()));
        } else if (sqlNode instanceof SqlKillConnection) {
            SqlKillConnection killConnection = (SqlKillConnection) sqlNode;
            return Optional.of(new KillConnection(killConnection.getThreadId()));
        } else if (sqlNode instanceof SqlShowEngines) {
            SqlShowEngines sqlShowEngines = (SqlShowEngines) sqlNode;
            return Optional.of(new ShowEnginesExecutor(sqlShowEngines.sqlLikePattern));
        } else if (sqlNode instanceof SqlShowPlugins) {
            SqlShowPlugins sqlShowPlugins = (SqlShowPlugins) sqlNode;
            return Optional.of(new ShowPluginsExecutor(sqlShowPlugins.sqlLikePattern));
        } else if (sqlNode instanceof SqlShowCollation) {
            SqlShowCollation sqlShowCollation = (SqlShowCollation) sqlNode;
            return Optional.of(new ShowCollationExecutor(sqlShowCollation.sqlLikePattern));
        } else if (sqlNode instanceof SqlShowCharset) {
            SqlShowCharset sqlShowCharset = (SqlShowCharset) sqlNode;
            return Optional.of(new ShowCharsetExecutor(sqlShowCharset.sqlLikePattern));
        } else if (sqlNode instanceof SqlShowLocks) {
            return Optional.of(new ShowLocksExecutor(
                ((SqlShowLocks) sqlNode).filterIdentifier,
                ((SqlShowLocks) sqlNode).filterKind,
                ((SqlShowLocks) sqlNode).filterOperand)
            );
        } else if (sqlNode instanceof SqlCall) {
            SqlCall sqlCall = (SqlCall) sqlNode;
            if (sqlCall.getCall().names.size() == 2) {
                String operation = sqlCall.getCall().names.get(1);
                if (operation.equalsIgnoreCase("getClientInfo")) {
                    return Optional.of(new SqlCallGetClientInfoExecutor(connection, sqlCall));
                } else if (operation.equalsIgnoreCase("setClientInfo")) {
                    return Optional.of(new SqlCallClientInfoExecutor(connection, (SqlCall) sqlNode));
                }
            }
            return Optional.empty();
        } else if (sqlNode instanceof SqlLoadData) {
            SqlLoadData sqlLoadData = (SqlLoadData) sqlNode;
            if (StringUtils.isBlank(sqlLoadData.getSchemaName())) {
                sqlLoadData.setSchemaName(getSchemaName(context));
            }
            return Optional.of(new LoadDataExecutor(sqlLoadData, connection, context));
        } else if (sqlNode instanceof SqlShowProcessList) {
            SqlShowProcessList showProcessList = (SqlShowProcessList) sqlNode;
            String user = context.getOption("user");
            String host = context.getOption("host");
            return Optional.of(new ShowProcessListExecutor(showProcessList.isProcessPrivilege(), user, host));
        } else if (sqlNode instanceof SqlShowTriggers) {
            SqlShowTriggers triggers = (SqlShowTriggers) sqlNode;
            return Optional.of(new ShowTriggersExecutor(triggers.sqlLikePattern));
        } else if (sqlNode instanceof SqlShowStartTs) {
            return Optional.of(new ShowStartTsExecutor());
        } else if (sqlNode instanceof SqlShowStatus) {
            SqlShowStatus sqlShowStatus = (SqlShowStatus) sqlNode;
            return Optional.of(new ShowStatusExecutor(sqlShowStatus.sqlLikePattern));
        } else if (sqlNode instanceof SqlShowTenants) {
            return Optional.of(new ShowTenantExecutor());
        } else if (sqlNode instanceof SqlShowExecutors) {
            return Optional.of(new ShowExecutorsExecutor());
        } else if (sqlNode instanceof SqlShowIndexFromTable) {
            SqlShowIndexFromTable showIndexFromTable = (SqlShowIndexFromTable) sqlNode;
            if (StringUtils.isEmpty(showIndexFromTable.schemaName)) {
                showIndexFromTable.schemaName = getSchemaName(context);
            }
            return Optional.of(new ShowIndexFromTableExecutor(showIndexFromTable));
        } else if (sqlNode instanceof SqlAdminRollback) {
            SqlAdminRollback sqlAdminRollback = (SqlAdminRollback) sqlNode;
            return Optional.of(new AdminRollbackExecutor(sqlAdminRollback.txnId));
        } else if (sqlNode instanceof SqlStartGc) {
            return Optional.of(new AdminStartGcExecutor());
        } else if (sqlNode instanceof SqlBackUpTimePoint) {
            SqlBackUpTimePoint sqlBackUpTimePoint = (SqlBackUpTimePoint) sqlNode;
            return Optional.of(new AdminBackUpTimePointExecutor(sqlBackUpTimePoint.timeStr));
        } else if (sqlNode instanceof SqlBackUpTsoPoint) {
            SqlBackUpTsoPoint sqlBackUpTsoPoint = (SqlBackUpTsoPoint) sqlNode;
            return Optional.of(new AdminBackUpTsoPointExecutor(sqlBackUpTsoPoint.point));
        } else {
            return Optional.empty();
        }
    }

    private static String getSchemaName(DingoParserContext context) {
        if (context.getUsedSchema() == null) {
            return context.getDefaultSchemaName();
        }
        return context.getUsedSchema().getName();
    }

}
