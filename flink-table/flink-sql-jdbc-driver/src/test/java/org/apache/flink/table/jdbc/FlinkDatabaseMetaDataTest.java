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

package org.apache.flink.table.jdbc;

import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for flink database metadata. */
public class FlinkDatabaseMetaDataTest extends FlinkJdbcDriverTestBase {

    @Test
    public void testGetColumns() throws Exception {
        DriverUri driverUri = getDriverUri();
        try (FlinkConnection connection = new FlinkConnection(driverUri)) {
            Executor executor = connection.getExecutor();
            // create databases in default catalog
            executeDDL("CREATE DATABASE database11", executor);
            // executeDDL("use database11", executor);
            executeDDL(
                    "CREATE TABLE default_catalog.database11.tb_columns ("
                            + "f0 CHAR comment 'this is comment',"
                            + "f1 CHAR,"
                            + "f2 CHAR(12),"
                            + "f3 VARCHAR,"
                            + "f4 VARCHAR(12),"
                            + "f5 STRING,"
                            + "f6 BOOLEAN,"
                            + "f7 BINARY,"
                            + "f8 VARBINARY,"
                            + "f9 VARBINARY(12),"
                            + "f10 BYTES,"
                            + "f11 DEC,"
                            + "f12 NUMERIC,"
                            + "f13 DECIMAL,"
                            + "f14 DECIMAL(11, 2),"
                            + "f15 TINYINT,"
                            + "f16 SMALLINT,"
                            + "f17 INT,"
                            + "f18 INTEGER,"
                            + "f19 BIGINT,"
                            + "f20 FLOAT,"
                            + "f21 DOUBLE,"
                            + "f22 DOUBLE PRECISION,"
                            + "f23 DATE,"
                            + "f24 TIME,"
                            + "f25 TIME(3),"
                            + "f26 TIMESTAMP,"
                            + "f27 TIMESTAMP(3),"
                            + "f28 TIMESTAMP_LTZ,"
                            + "f29 TIMESTAMP_LTZ(3),"
                            + "f30 BIGINT,"
                            + "primary key (f0, f1, f2) not enforced"
                            + ") WITH ('connector' = 'datagen')",
                    executor);

            DatabaseMetaData metaData = connection.getMetaData();
            List<String> strings =
                    resultSetToListAndClose(
                            metaData.getColumns(
                                    "default_catalog", "database11", "tb_columns", "%"));
            strings.forEach(System.out::println);
        }
    }

    @Test
    public void testGetTables() throws Exception {
        DriverUri driverUri = getDriverUri();
        try (FlinkConnection connection = new FlinkConnection(driverUri)) {
            Executor executor = connection.getExecutor();
            // create databases in default catalog
            executeDDL("CREATE DATABASE database11", executor);
            executeDDL("CREATE DATABASE database12", executor);
            // executeDDL("use database11", executor);
            executeDDL(
                    "CREATE TABLE default_catalog.database11.orders (a BIGINT) WITH ('connector' = 'datagen')",
                    executor);
            executeDDL(
                    "CREATE TABLE default_catalog.database11.users (a BIGINT) WITH ('connector' = 'datagen')",
                    executor);
            executeDDL("CREATE VIEW default_catalog.database11.thing_v as select 1", executor);

            // executeDDL("use database12", executor);
            executeDDL("CREATE VIEW default_catalog.database12.orders_v as select 1", executor);

            executeDDL("CREATE CATALOG catalog2 WITH ('type'='generic_in_memory');", executor);
            executeDDL("CREATE DATABASE catalog2.db2", executor);
            // executeDDL("use catalog catalog2", executor);
            // executeDDL("use db2", executor);
            executeDDL(
                    "CREATE TABLE catalog2.db2.tb1 (a BIGINT) WITH ('connector' = 'datagen')",
                    executor);

            DatabaseMetaData metaData = connection.getMetaData();
            assertThat(
                            resultSetToListAndClose(
                                    metaData.getTables("default_catalog", "database11", "%", null)))
                    .containsExactly(
                            "default_catalog,database11,orders,TABLE,,,,,,",
                            "default_catalog,database11,thing_v,VIEW,,,,,,",
                            "default_catalog,database11,users,TABLE,,,,,,");
            assertThat(
                            resultSetToListAndClose(
                                    metaData.getTables("default_catalog", null, null, null)))
                    .containsExactly(
                            "default_catalog,database11,orders,TABLE,,,,,,",
                            "default_catalog,database11,thing_v,VIEW,,,,,,",
                            "default_catalog,database11,users,TABLE,,,,,,",
                            "default_catalog,database12,orders_v,VIEW,,,,,,");
            assertThat(
                            resultSetToListAndClose(
                                    metaData.getTables(
                                            "default_catalog", null, null, new String[] {"TABLE"})))
                    .containsExactly(
                            "default_catalog,database11,orders,TABLE,,,,,,",
                            "default_catalog,database11,users,TABLE,,,,,,");
            assertThat(
                            resultSetToListAndClose(
                                    metaData.getTables(
                                            "default_catalog", null, null, new String[] {"VIEW"})))
                    .containsExactly(
                            "default_catalog,database11,thing_v,VIEW,,,,,,",
                            "default_catalog,database12,orders_v,VIEW,,,,,,");
            assertThat(
                            resultSetToListAndClose(
                                    metaData.getTables(
                                            "default_catalog",
                                            "database11",
                                            null,
                                            new String[] {"TABLE"})))
                    .containsExactly(
                            "default_catalog,database11,orders,TABLE,,,,,,",
                            "default_catalog,database11,users,TABLE,,,,,,");
            assertThat(
                            resultSetToListAndClose(
                                    metaData.getTables(
                                            "default_catalog",
                                            "database11",
                                            null,
                                            new String[] {"VIEW"})))
                    .containsExactly("default_catalog,database11,thing_v,VIEW,,,,,,");
            assertThat(
                            resultSetToListAndClose(
                                    metaData.getTables(
                                            "default_catalog",
                                            "database11",
                                            "ord%",
                                            new String[] {"TABLE"})))
                    .containsExactly("default_catalog,database11,orders,TABLE,,,,,,");
        }
    }

    @Test
    public void testCatalogSchemas() throws Exception {

        DriverUri driverUri = getDriverUri();
        try (FlinkConnection connection = new FlinkConnection(driverUri)) {
            Executor executor = connection.getExecutor();
            // create databases in default catalog
            executeDDL("CREATE DATABASE database11", executor);
            executeDDL("CREATE DATABASE database12", executor);
            executeDDL("CREATE DATABASE database13", executor);

            // create catalog2 and databases
            executeDDL("CREATE CATALOG test_catalog2 WITH ('type'='generic_in_memory');", executor);
            executeDDL("CREATE DATABASE test_catalog2.database11", executor);
            executeDDL("CREATE DATABASE test_catalog2.database21", executor);
            executeDDL("CREATE DATABASE test_catalog2.database31", executor);

            // create catalog1 and databases
            executeDDL("CREATE CATALOG test_catalog1 WITH ('type'='generic_in_memory');", executor);
            executeDDL("CREATE DATABASE test_catalog1.database11", executor);
            executeDDL("CREATE DATABASE test_catalog1.database21", executor);
            executeDDL("CREATE DATABASE test_catalog1.database13", executor);

            connection.setCatalog("test_catalog2");
            connection.setSchema("database21");
            assertEquals("test_catalog2", connection.getCatalog());
            assertEquals("database21", connection.getSchema());

            DatabaseMetaData databaseMetaData =
                    new FlinkDatabaseMetaData(
                            driverUri.getURL(), connection, new TestingStatement());
            // Show all catalogs
            assertThat(resultSetToListAndClose(databaseMetaData.getCatalogs()))
                    .containsExactly("default_catalog", "test_catalog1", "test_catalog2");
            // Show all databases
            assertThat(resultSetToListAndClose(databaseMetaData.getSchemas()))
                    .containsExactly(
                            "database11,default_catalog",
                            "database12,default_catalog",
                            "database13,default_catalog",
                            "default_database,default_catalog",
                            "database11,test_catalog1",
                            "database13,test_catalog1",
                            "database21,test_catalog1",
                            "default,test_catalog1",
                            "database11,test_catalog2",
                            "database21,test_catalog2",
                            "database31,test_catalog2",
                            "default,test_catalog2");

            // Validate that the default catalog and database are not changed.
            assertEquals("test_catalog2", connection.getCatalog());
            assertEquals("database21", connection.getSchema());

            assertThat(resultSetToListAndClose(databaseMetaData.getSchemas("test_catalog2", null)))
                    .containsExactly("database11", "database21", "database31", "default");

            assertFalse(databaseMetaData.allProceduresAreCallable());
            assertTrue(databaseMetaData.allTablesAreSelectable());
            assertTrue(databaseMetaData.isReadOnly());
            assertFalse(databaseMetaData.nullsAreSortedHigh());
            assertTrue(databaseMetaData.nullsAreSortedLow());
            assertFalse(databaseMetaData.nullsAreSortedAtStart());
            assertFalse(databaseMetaData.nullsAreSortedAtEnd());
            assertFalse(databaseMetaData.usesLocalFiles());
            assertFalse(databaseMetaData.usesLocalFilePerTable());
            assertTrue(databaseMetaData.supportsMixedCaseIdentifiers());
            assertFalse(databaseMetaData.storesUpperCaseIdentifiers());
            assertFalse(databaseMetaData.storesLowerCaseIdentifiers());
            assertTrue(databaseMetaData.storesMixedCaseIdentifiers());
            assertTrue(databaseMetaData.supportsMixedCaseQuotedIdentifiers());
            assertFalse(databaseMetaData.storesUpperCaseQuotedIdentifiers());
            assertFalse(databaseMetaData.storesLowerCaseQuotedIdentifiers());
            assertTrue(databaseMetaData.storesMixedCaseQuotedIdentifiers());
            assertTrue(databaseMetaData.supportsAlterTableWithAddColumn());
            assertTrue(databaseMetaData.supportsAlterTableWithDropColumn());
            assertTrue(databaseMetaData.supportsColumnAliasing());
            assertTrue(databaseMetaData.nullPlusNonNullIsNull());
            assertFalse(databaseMetaData.supportsConvert());
            assertFalse(databaseMetaData.supportsConvert(1, 2));
            assertTrue(databaseMetaData.supportsTableCorrelationNames());
            assertFalse(databaseMetaData.supportsDifferentTableCorrelationNames());
            assertTrue(databaseMetaData.supportsExpressionsInOrderBy());
            assertTrue(databaseMetaData.supportsOrderByUnrelated());
            assertTrue(databaseMetaData.supportsGroupBy());
            assertTrue(databaseMetaData.supportsGroupByUnrelated());
            assertTrue(databaseMetaData.supportsGroupByBeyondSelect());
            assertTrue(databaseMetaData.supportsLikeEscapeClause());
            assertFalse(databaseMetaData.supportsMultipleResultSets());
            assertFalse(databaseMetaData.supportsMultipleTransactions());
            assertTrue(databaseMetaData.supportsNonNullableColumns());
            assertFalse(databaseMetaData.supportsMinimumSQLGrammar());
            assertFalse(databaseMetaData.supportsCoreSQLGrammar());
            assertFalse(databaseMetaData.supportsExtendedSQLGrammar());
            assertFalse(databaseMetaData.supportsANSI92EntryLevelSQL());
            assertFalse(databaseMetaData.supportsANSI92IntermediateSQL());
            assertFalse(databaseMetaData.supportsANSI92FullSQL());
            assertFalse(databaseMetaData.supportsIntegrityEnhancementFacility());
            assertTrue(databaseMetaData.supportsOuterJoins());
            assertTrue(databaseMetaData.supportsFullOuterJoins());
            assertTrue(databaseMetaData.supportsLimitedOuterJoins());
            assertTrue(databaseMetaData.isCatalogAtStart());
            assertTrue(databaseMetaData.supportsSchemasInDataManipulation());
            assertFalse(databaseMetaData.supportsSchemasInProcedureCalls());
            assertTrue(databaseMetaData.supportsSchemasInTableDefinitions());
            assertFalse(databaseMetaData.supportsSchemasInIndexDefinitions());
            assertFalse(databaseMetaData.supportsSchemasInPrivilegeDefinitions());
            assertTrue(databaseMetaData.supportsCatalogsInDataManipulation());
            assertFalse(databaseMetaData.supportsCatalogsInProcedureCalls());
            assertTrue(databaseMetaData.supportsCatalogsInTableDefinitions());
            assertFalse(databaseMetaData.supportsCatalogsInIndexDefinitions());
            assertFalse(databaseMetaData.supportsCatalogsInPrivilegeDefinitions());
            assertFalse(databaseMetaData.supportsPositionedDelete());
            assertFalse(databaseMetaData.supportsPositionedUpdate());
            assertFalse(databaseMetaData.supportsSelectForUpdate());
            assertFalse(databaseMetaData.supportsStoredProcedures());
            assertTrue(databaseMetaData.supportsSubqueriesInComparisons());
            assertTrue(databaseMetaData.supportsSubqueriesInExists());
            assertTrue(databaseMetaData.supportsSubqueriesInIns());
            assertTrue(databaseMetaData.supportsSubqueriesInQuantifieds());
            assertTrue(databaseMetaData.supportsCorrelatedSubqueries());
            assertTrue(databaseMetaData.supportsUnion());
            assertTrue(databaseMetaData.supportsUnionAll());
            assertFalse(databaseMetaData.supportsOpenCursorsAcrossCommit());
            assertFalse(databaseMetaData.supportsOpenCursorsAcrossRollback());
            assertFalse(databaseMetaData.supportsOpenStatementsAcrossCommit());
            assertFalse(databaseMetaData.supportsOpenStatementsAcrossRollback());
            assertFalse(databaseMetaData.doesMaxRowSizeIncludeBlobs());
            assertFalse(databaseMetaData.supportsTransactions());
            assertFalse(databaseMetaData.supportsTransactionIsolationLevel(1));
            assertFalse(databaseMetaData.supportsDataDefinitionAndDataManipulationTransactions());
            assertFalse(databaseMetaData.supportsDataManipulationTransactionsOnly());
            assertFalse(databaseMetaData.dataDefinitionCausesTransactionCommit());
            assertFalse(databaseMetaData.dataDefinitionIgnoredInTransactions());
            assertFalse(databaseMetaData.supportsResultSetType(1));
            assertFalse(databaseMetaData.supportsResultSetConcurrency(1, 2));
            assertFalse(databaseMetaData.ownUpdatesAreVisible(1));
            assertFalse(databaseMetaData.ownDeletesAreVisible(1));
            assertFalse(databaseMetaData.ownInsertsAreVisible(1));
            assertFalse(databaseMetaData.othersUpdatesAreVisible(1));
            assertFalse(databaseMetaData.othersDeletesAreVisible(1));
            assertFalse(databaseMetaData.othersInsertsAreVisible(1));
            assertFalse(databaseMetaData.updatesAreDetected(1));
            assertFalse(databaseMetaData.deletesAreDetected(1));
            assertFalse(databaseMetaData.insertsAreDetected(1));
            assertFalse(databaseMetaData.supportsBatchUpdates());
            assertFalse(databaseMetaData.supportsSavepoints());
            assertFalse(databaseMetaData.supportsNamedParameters());
            assertFalse(databaseMetaData.supportsMultipleOpenResults());
            assertFalse(databaseMetaData.supportsGetGeneratedKeys());
            assertFalse(databaseMetaData.supportsResultSetHoldability(1));
            assertFalse(databaseMetaData.locatorsUpdateCopy());
            assertFalse(databaseMetaData.supportsStatementPooling());
            assertFalse(databaseMetaData.supportsStoredFunctionsUsingCallSyntax());
            assertFalse(databaseMetaData.autoCommitFailureClosesAllResultSets());
            assertFalse(databaseMetaData.generatedKeyAlwaysReturned());
        }
    }

    private List<String> resultSetToListAndClose(ResultSet resultSet) throws Exception {
        List<String> resultList = new ArrayList<>();
        int columnCount = resultSet.getMetaData().getColumnCount();
        while (resultSet.next()) {
            List<String> columnStringList = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                switch (resultSet.getMetaData().getColumnType(i)) {
                    case Types.INTEGER:
                        columnStringList.add(String.valueOf(resultSet.getInt(i)));
                        break;
                    case Types.BIGINT:
                        columnStringList.add(String.valueOf(resultSet.getLong(i)));
                        break;
                    default:
                        columnStringList.add(resultSet.getString(i));
                        break;
                }
            }
            resultList.add(StringUtils.join(columnStringList, ","));
        }
        resultSet.close();

        return resultList;
    }

    private void executeDDL(String sql, Executor executor) {
        try (StatementResult result = executor.executeStatement(sql)) {
            assertTrue(result.hasNext());
            RowData rowData = result.next();
            assertEquals(1, rowData.getArity());
            assertEquals("OK", rowData.getString(0).toString());

            assertFalse(result.hasNext());
        }
    }
}
