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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import javax.annotation.Nullable;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.table.jdbc.utils.DatabaseMetaDataUtils.createCatalogsResultSet;
import static org.apache.flink.table.jdbc.utils.DatabaseMetaDataUtils.createClientInfoPropertiesResultSet;
import static org.apache.flink.table.jdbc.utils.DatabaseMetaDataUtils.createColumnsResultSet;
import static org.apache.flink.table.jdbc.utils.DatabaseMetaDataUtils.createPrimaryKeysResultSet;
import static org.apache.flink.table.jdbc.utils.DatabaseMetaDataUtils.createSchemasResultSet;
import static org.apache.flink.table.jdbc.utils.DatabaseMetaDataUtils.createTablesResultSet;

/** Implementation of {@link java.sql.DatabaseMetaData} for flink jdbc driver. */
public class FlinkDatabaseMetaData extends BaseDatabaseMetaData {
    private final String url;
    private final FlinkConnection connection;
    private final Statement statement;
    private final Executor executor;

    private void writeLog(String msg) {
        String file = "C:\\tmp\\log";
        if (!new File(file).exists()) {
            // for debug log
            return;
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        msg = simpleDateFormat.format(new Date()) + " " + msg + "\n";

        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(file, true));
            out.write(msg);
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @VisibleForTesting
    protected FlinkDatabaseMetaData(String url, FlinkConnection connection, Statement statement) {
        this.url = url;
        this.connection = connection;
        this.statement = statement;
        this.executor = connection.getExecutor();
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return createClientInfoPropertiesResultSet(statement);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        try (StatementResult result = catalogs()) {
            return createCatalogsResultSet(statement, result);
        } catch (Exception e) {
            throw new SQLException("Get catalogs fail", e);
        }
    }

    private StatementResult catalogs() {
        return executor.executeStatement("SHOW CATALOGS");
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        writeLog("getSchemas()");
        try {
            String currentCatalog = connection.getCatalog();
            String currentDatabase = connection.getSchema();
            List<String> catalogList = new ArrayList<>();
            Map<String, List<String>> catalogSchemaList = new HashMap<>();
            try (StatementResult result = catalogs()) {
                while (result.hasNext()) {
                    String catalog = result.next().getString(0).toString();
                    connection.setCatalog(catalog);
                    getSchemasForCatalog(catalogList, catalogSchemaList, catalog, null);
                }
            }
            connection.setCatalog(currentCatalog);
            connection.setSchema(currentDatabase);

            return createSchemasResultSet(statement, catalogList, catalogSchemaList);
        } catch (Exception e) {
            throw new SQLException("Get schemas fail", e);
        }
    }

    private void getSchemasForCatalog(
            List<String> catalogList,
            Map<String, List<String>> catalogSchemaList,
            String catalog,
            @Nullable String schemaPattern)
            throws SQLException {
        catalogList.add(catalog);
        List<String> schemas = new ArrayList<>();
        try (StatementResult schemaResult = schemas()) {
            while (schemaResult.hasNext()) {
                String schema = schemaResult.next().getString(0).toString();
                // TODO: support as SHOW DATABASES FROM xx LIKE 'schemaPattern'
                schemas.add(schema);
            }
        }
        catalogSchemaList.put(catalog, schemas);
    }

    private StatementResult schemas() {
        return executor.executeStatement("SHOW DATABASES;");
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        // TODO Flink will support SHOW DATABASES LIKE statement in FLIP-297, this method will be
        // re-supported after that issue.
        writeLog(String.format("getSchemas(%s, %s)", catalog, schemaPattern));
        try {
            String currentCatalog = connection.getCatalog();
            String currentDatabase = connection.getSchema();
            List<String> catalogList = new ArrayList<>();
            Map<String, List<String>> catalogSchemaList = new HashMap<>();
            connection.setCatalog(catalog);
            getSchemasForCatalog(catalogList, catalogSchemaList, catalog, schemaPattern);
            connection.setCatalog(currentCatalog);
            connection.setSchema(currentDatabase);

            return createSchemasResultSet(statement, catalogList, catalogSchemaList);
        } catch (Exception e) {
            throw new SQLException("Get schemas fail", e);
        }
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getTables(
            String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        writeLog(
                String.format(
                        "getTables(%s, %s, %s, %s)",
                        catalog,
                        schemaPattern,
                        tableNamePattern,
                        types == null ? "" : String.join("|", types)));
        try {
            String currentCatalog = connection.getCatalog();
            String currentDatabase = connection.getSchema();
            List<String> schemasList = new ArrayList<>();
            Map<String, List<String>> schemaTables = new HashMap<>();
            Map<String, String[]> tablesInfo = new HashMap<>();
            connection.setCatalog(catalog);
            try (StatementResult schemasResult = schemas()) {
                while (schemasResult.hasNext()) {
                    String schema = schemasResult.next().getString(0).toString();
                    if (schemaPattern == null || schema.equals(schemaPattern)) {
                        schemasList.add(schema);
                        connection.setSchema(schema);
                        getTablesForCatalog(
                                schemaTables, tablesInfo, catalog, schema, tableNamePattern, types);
                    }
                }
            }

            connection.setCatalog(currentCatalog);
            connection.setSchema(currentDatabase);

            return createTablesResultSet(statement, schemasList, schemaTables, tablesInfo);
        } catch (Exception e) {
            throw new SQLException("Get tables fail", e);
        }
    }

    private void getTablesForCatalog(
            Map<String, List<String>> schemaTables,
            Map<String, String[]> tablesInfo,
            String catalog,
            String schema,
            String tableNamePattern,
            String[] types) {
        List<String> tables = new ArrayList<>();
        schemaTables.put(schema, tables);
        List<String> viewsInSchema = new ArrayList<>();
        boolean containView =
                types == null || Arrays.stream(types).anyMatch(x -> x.equalsIgnoreCase("VIEW"));
        try (StatementResult viewsResult = views()) {
            while (viewsResult.hasNext()) {
                String view = viewsResult.next().getString(0).toString();
                String viewNameTag = view + "#V";
                viewsInSchema.add(viewNameTag);
                if (containView) {
                    tables.add(viewNameTag);
                    tablesInfo.put(
                            schema + "." + viewNameTag,
                            new String[] {catalog, schema, view, "VIEW"});
                }
            }
        }
        if (types == null || Arrays.stream(types).anyMatch(x -> x.equalsIgnoreCase("TABLE"))) {
            try (StatementResult tablesResult = tables(tableNamePattern)) {
                while (tablesResult.hasNext()) {
                    String table = tablesResult.next().getString(0).toString();
                    if (viewsInSchema.contains(table + "#V")) {
                        // because SHOW TABLES; result include views
                        continue;
                    }
                    String tableNameTag = table + "#T";
                    tables.add(tableNameTag);
                    tablesInfo.put(
                            schema + "." + tableNameTag,
                            new String[] {catalog, schema, table, "TABLE"});
                }
            }
        }
    }

    private StatementResult views() {
        return executor.executeStatement("SHOW VIEWS;");
    }

    private StatementResult tables(String tableNamePattern) {
        if (tableNamePattern == null || tableNamePattern.isEmpty()) {
            return executor.executeStatement("SHOW TABLES;");
        }
        return executor.executeStatement(String.format("SHOW TABLES LIKE '%s';", tableNamePattern));
    }

    @Override
    public ResultSet getColumns(
            String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        writeLog(
                String.format(
                        "getColumns(%s, %s, %s, %s)",
                        catalog, schemaPattern, tableNamePattern, columnNamePattern));
        try (StatementResult describeResult = columns(catalog, schemaPattern, tableNamePattern)) {
            Map<String, String[]> columnInfo = new HashMap<>();
            List<String> ordinalPositions = new ArrayList<>();
            int ordinalPosition = 1;
            StringData tempStringData = null;
            while (describeResult.hasNext()) {
                RowData rowData = describeResult.next();
                String name = rowData.getString(0).toString();
                String type = rowData.getString(1).toString();
                boolean nullable = rowData.getBoolean(2);
                tempStringData = rowData.getString(3);
                String key = tempStringData == null ? null : tempStringData.toString();
                tempStringData = rowData.getString(4);
                String extras = tempStringData == null ? null : tempStringData.toString();
                tempStringData = rowData.getString(5);
                String watermark = tempStringData == null ? null : tempStringData.toString();
                String comment = null;
                if (rowData.getArity() == 7) {
                    tempStringData = rowData.getString(6);
                    comment = tempStringData == null ? null : tempStringData.toString();
                }
                ordinalPositions.add(name);
                ordinalPosition++;
                if (key != null) {
                    // append primary key information to comment for DBeaver
                    comment = comment == null ? "[PK]" : String.format("[PK] %s", comment);
                }
                columnInfo.put(
                        name,
                        new String[] {
                            name, type, nullable ? "YES" : "NO", key, extras, watermark, comment
                        });
            }

            return createColumnsResultSet(
                    catalog,
                    schemaPattern,
                    tableNamePattern,
                    statement,
                    ordinalPositions,
                    columnInfo);
        } catch (Exception e) {
            throw new SQLException("Get Columns fail", e);
        }
    }

    private StatementResult columns(String catalog, String schema, String table) {
        return executor.executeStatement(
                String.format("DESCRIBE `%s`.`%s`.`%s`;", catalog, schema, table));
    }

    private static final Pattern CONSTRAINT_PATTERN =
            Pattern.compile(".*CONSTRAINT `([^`]+)` PRIMARY KEY \\(([^)]+)\\) NOT ENFORCED.*");

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table)
            throws SQLException {
        try (StatementResult ddlResult =
                executor.executeStatement(
                        String.format(
                                "SHOW CREATE TABLE `%s`.`%s`.`%s`", catalog, schema, table))) {
            String ddl = ddlResult.next().getString(0).toString();
            List<String> columnList = new ArrayList<>();
            Map<String, String> columnAndPkNames = new HashMap<>();
            if (ddl.contains("CONSTRAINT")) {
                Matcher matcher = CONSTRAINT_PATTERN.matcher(ddl);
                if (matcher.find()) {
                    String pknName = matcher.group(1);
                    String columns = matcher.group(2);
                    String[] split = columns.split(",");
                    for (int i = 0; i < split.length; i++) {
                        String column = split[i].trim().replace("`", "");
                        columnAndPkNames.put(column, pknName);
                        columnList.add(column);
                    }
                } else {
                    throw new RuntimeException("Pattern no match ddl");
                }
            }
            return createPrimaryKeysResultSet(
                    catalog, schema, table, statement, columnList, columnAndPkNames);
        } catch (Exception e) {
            throw new SQLException("Get PrimaryKeys fail", e);
        }
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return url;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    /** In flink null value will be used as low value for sort. */
    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return DriverInfo.DRIVER_NAME;
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return DriverInfo.DRIVER_VERSION;
    }

    @Override
    public String getDriverName() throws SQLException {
        return FlinkDriver.class.getName();
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return DriverInfo.DRIVER_VERSION;
    }

    @Override
    public int getDriverMajorVersion() {
        return DriverInfo.DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getDriverMinorVersion() {
        return DriverInfo.DRIVER_VERSION_MINOR;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return DriverInfo.DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return DriverInfo.DRIVER_VERSION_MINOR;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return DriverInfo.DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return DriverInfo.DRIVER_VERSION_MINOR;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    /** Flink sql is mixed case as sensitive. */
    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    /** Flink sql is mixed case as sensitive. */
    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    /** Flink sql is mixed case as sensitive. */
    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    /** Flink sql is mixed case as sensitive. */
    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "`";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    /** Null value plus non-null in flink will be null result. */
    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "database";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "catalog";
    }

    /** Catalog name appears at the start of full name. */
    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
