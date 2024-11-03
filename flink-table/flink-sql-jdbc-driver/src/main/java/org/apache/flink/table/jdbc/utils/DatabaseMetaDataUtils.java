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

package org.apache.flink.table.jdbc.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.jdbc.FlinkDatabaseMetaData;
import org.apache.flink.table.jdbc.FlinkResultSet;

import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utils to create catalog/schema results for {@link FlinkDatabaseMetaData}. */
public class DatabaseMetaDataUtils {
    private static final Column TABLE_CAT_COLUMN =
            Column.physical("TABLE_CAT", DataTypes.STRING().notNull());
    private static final Column TABLE_SCHEM_COLUMN =
            Column.physical("TABLE_SCHEM", DataTypes.STRING().notNull());
    private static final Column TABLE_CATALOG_COLUMN =
            Column.physical("TABLE_CATALOG", DataTypes.STRING());
    private static final Column TABLE_NAME_COLUMN =
            Column.physical("TABLE_NAME", DataTypes.STRING());
    private static final Column TABLE_TYPE_COLUMN =
            Column.physical("TABLE_TYPE", DataTypes.STRING());
    private static final Column REMARKS_COLUMN = Column.physical("REMARKS", DataTypes.STRING());
    private static final Column TYPE_CAT_COLUMN = Column.physical("TYPE_CAT", DataTypes.STRING());
    private static final Column TYPE_SCHEM_COLUMN =
            Column.physical("TYPE_SCHEM", DataTypes.STRING());
    private static final Column TYPE_NAME_COLUMN = Column.physical("TYPE_NAME", DataTypes.STRING());
    private static final Column SELF_REFERENCING_COL_NAME_COLUMN =
            Column.physical("SELF_REFERENCING_COL_NAME", DataTypes.STRING());
    private static final Column REF_GENERATION_COLUMN =
            Column.physical("REF_GENERATION", DataTypes.STRING());
    private static final Column COLUMN_NAME_COLUMN =
            Column.physical("COLUMN_NAME", DataTypes.STRING());
    private static final Column DATA_TYPE_COLUMN = Column.physical("DATA_TYPE", DataTypes.INT());
    private static final Column COLUMN_SIZE_COLUMN =
            Column.physical("COLUMN_SIZE", DataTypes.INT());
    private static final Column LENGTH_COLUMN = Column.physical("LENGTH", DataTypes.INT());
    private static final Column BUFFER_LENGTH_COLUMN =
            Column.physical("BUFFER_LENGTH", DataTypes.STRING());
    private static final Column DECIMAL_DIGITS_COLUMN =
            Column.physical("DECIMAL_DIGITS", DataTypes.INT());
    private static final Column NUM_PREC_RADIX_COLUMN =
            Column.physical("NUM_PREC_RADIX", DataTypes.INT());
    private static final Column NULLABLE_COLUMN = Column.physical("NULLABLE", DataTypes.INT());
    private static final Column COLUMN_DEF_COLUMN =
            Column.physical("COLUMN_DEF", DataTypes.STRING());
    private static final Column SQL_DATA_TYPE_COLUMN =
            Column.physical("SQL_DATA_TYPE", DataTypes.INT());
    private static final Column SQL_DATETIME_SUB_COLUMN =
            Column.physical("SQL_DATETIME_SUB", DataTypes.INT());
    private static final Column CHAR_OCTET_LENGTH_COLUMN =
            Column.physical("CHAR_OCTET_LENGTH", DataTypes.INT());
    private static final Column ORDINAL_POSITION_COLUMN =
            Column.physical("ORDINAL_POSITION", DataTypes.INT());
    private static final Column IS_NULLABLE_COLUMN =
            Column.physical("IS_NULLABLE", DataTypes.STRING());
    private static final Column SCOPE_CATALOG_COLUMN =
            Column.physical("SCOPE_CATALOG", DataTypes.STRING());
    private static final Column SCOPE_SCHEMA_COLUMN =
            Column.physical("SCOPE_SCHEMA", DataTypes.STRING());
    private static final Column SCOPE_TABLE_COLUMN =
            Column.physical("SCOPE_TABLE", DataTypes.STRING());
    private static final Column SOURCE_DATA_TYPE_COLUMN =
            Column.physical("SOURCE_DATA_TYPE", DataTypes.STRING());
    private static final Column IS_AUTOINCREMENT_COLUMN =
            Column.physical("IS_AUTOINCREMENT", DataTypes.STRING());
    private static final Column IS_GENERATEDCOLUMN_COLUMN =
            Column.physical("IS_GENERATEDCOLUMN", DataTypes.STRING());
    private static final Column KEY_SEQ_COLUMN = Column.physical("KEY_SEQ", DataTypes.STRING());
    private static final Column PK_NAME_COLUMN = Column.physical("PK_NAME", DataTypes.STRING());
    private static final Column NAME_COLUMN = Column.physical("NAME", DataTypes.STRING());
    private static final Column MAX_LEN_COLUMN = Column.physical("MAX_LEN", DataTypes.INT());
    private static final Column DEFAULT_VALUE_COLUMN =
            Column.physical("DEFAULT_VALUE", DataTypes.STRING());
    private static final Column DESCRIPTION_COLUMN =
            Column.physical("DESCRIPTION", DataTypes.STRING());

    public static FlinkResultSet createClientInfoPropertiesResultSet(Statement statement) {
        List<RowData> info = new ArrayList<>();
        info.add(
                GenericRowData.of(
                        StringData.fromString("waitJobTerminalState"),
                        1024,
                        StringData.fromString("false"),
                        StringData.fromString(
                                "Will not return JobID When execution INSERT statement")));
        info.sort(Comparator.comparing(v -> v.getString(0)));

        return new FlinkResultSet(
                statement,
                new CollectionResultIterator(info.iterator()),
                ResolvedSchema.of(
                        NAME_COLUMN, MAX_LEN_COLUMN, DEFAULT_VALUE_COLUMN, DESCRIPTION_COLUMN));
    }

    /**
     * Create result set for catalogs. The schema columns are:
     *
     * <ul>
     *   <li>TABLE_CAT String => catalog name.
     * </ul>
     *
     * <p>The results are ordered by catalog name.
     *
     * @param statement The statement for database meta data
     * @param result The result for catalogs
     * @return a ResultSet object in which each row has a single String column that is a catalog
     *     name
     */
    public static FlinkResultSet createCatalogsResultSet(
            Statement statement, StatementResult result) {
        List<RowData> catalogs = new ArrayList<>();
        result.forEachRemaining(catalogs::add);
        catalogs.sort(Comparator.comparing(v -> v.getString(0)));

        return new FlinkResultSet(
                statement,
                new CollectionResultIterator(catalogs.iterator()),
                ResolvedSchema.of(TABLE_CAT_COLUMN));
    }

    /**
     * Create result set for schemas. The schema columns are:
     *
     * <ul>
     *   <li>TABLE_SCHEM String => schema name
     *   <li>TABLE_CATALOG String => catalog name (may be null)
     * </ul>
     *
     * <p>The results are ordered by TABLE_CATALOG and TABLE_SCHEM.
     *
     * @param statement The statement for database meta data
     * @param catalogs The catalog list
     * @param catalogSchemas The catalog with schema list
     * @return a ResultSet object in which each row is a schema description
     */
    public static FlinkResultSet createSchemasResultSet(
            Statement statement, List<String> catalogs, Map<String, List<String>> catalogSchemas) {
        List<RowData> schemaWithCatalogList = new ArrayList<>();
        List<String> catalogList = new ArrayList<>(catalogs);
        catalogList.sort(String::compareTo);
        for (String catalog : catalogList) {
            List<String> schemas = catalogSchemas.get(catalog);
            schemas.sort(String::compareTo);
            schemas.forEach(
                    s ->
                            schemaWithCatalogList.add(
                                    GenericRowData.of(
                                            StringData.fromString(s),
                                            StringData.fromString(catalog))));
        }

        return new FlinkResultSet(
                statement,
                new CollectionResultIterator(schemaWithCatalogList.iterator()),
                ResolvedSchema.of(TABLE_SCHEM_COLUMN, TABLE_CATALOG_COLUMN));
    }

    public static FlinkResultSet createPrimaryKeysResultSet(
            String catalog,
            String schema,
            String table,
            Statement statement,
            List<String> columnList,
            Map<String, String> columnAndPkNames) {
        List<RowData> pkRowDataList = new ArrayList<>();
        for (int i = 0; i < columnList.size(); i++) {
            String column = columnList.get(i);
            pkRowDataList.add(
                    GenericRowData.of(
                            StringData.fromString(catalog),
                            StringData.fromString(schema),
                            StringData.fromString(table),
                            StringData.fromString(column),
                            i,
                            columnAndPkNames.get(column)));
        }
        return new FlinkResultSet(
                statement,
                new CollectionResultIterator(pkRowDataList.iterator()),
                ResolvedSchema.of(
                        TABLE_CAT_COLUMN,
                        TABLE_SCHEM_COLUMN,
                        TABLE_NAME_COLUMN,
                        COLUMN_NAME_COLUMN,
                        KEY_SEQ_COLUMN,
                        PK_NAME_COLUMN));
    }

    public static FlinkResultSet createColumnsResultSet(
            String catalog,
            String schema,
            String tableName,
            Statement statement,
            List<String> ordinalPositions,
            Map<String, String[]> columnInfo) {
        List<RowData> columnRowDataList = new ArrayList<>();
        for (int i = 0; i < ordinalPositions.size(); i++) {
            String columnName = ordinalPositions.get(i);
            String[] info = columnInfo.get(columnName);
            Integer[] javaSqlType = flinkSqlTypeToJavaSqlType(info[1]);

            columnRowDataList.add(
                    GenericRowData.of(
                            StringData.fromString(catalog),
                            StringData.fromString(schema),
                            StringData.fromString(tableName),
                            StringData.fromString(info[0]),
                            javaSqlType[0],
                            StringData.fromString(info[1]),
                            StringData.fromString(info[6]),
                            null,
                            ordinalPositions.indexOf(columnName) + 1,
                            StringData.fromString(info[2]),
                            "NO".equals(info[2]) ? 0 : 1));
        }

        return new FlinkResultSet(
                statement,
                new CollectionResultIterator(columnRowDataList.iterator()),
                ResolvedSchema.of(
                        TABLE_CAT_COLUMN,
                        TABLE_SCHEM_COLUMN,
                        TABLE_NAME_COLUMN,
                        COLUMN_NAME_COLUMN,
                        DATA_TYPE_COLUMN,
                        TYPE_NAME_COLUMN,
                        REMARKS_COLUMN,
                        COLUMN_DEF_COLUMN,
                        ORDINAL_POSITION_COLUMN,
                        IS_NULLABLE_COLUMN,
                        NULLABLE_COLUMN));
    }

    private static final Pattern ZONE_PATTERN =
            Pattern.compile(
                    "([^(]*)(?:\\(([0-9, ]+)\\))?\\s(WITHOUT TIME ZONE|WITH TIME ZONE|WITH LOCAL TIME ZONE)");
    private static final Pattern UNZONE_PATTERN = Pattern.compile("([^(]*)(?:\\(([0-9, ]+)\\))?");

    public static Integer[] flinkSqlTypeToJavaSqlType(String flinkSqlType) {
        String namePart;
        String extPart1 = null; // varchar(256), e.g. 256
        String extPart2 = null; // TIMESTAMP(p) WITHOUT TIME ZONE, e.g. WITHOUT TIME ZONE
        String flinkSqlTypeUpperCase = flinkSqlType.toUpperCase(Locale.ENGLISH);
        if (flinkSqlTypeUpperCase.contains("ZONE")) {
            Matcher matcher = ZONE_PATTERN.matcher(flinkSqlTypeUpperCase);
            if (matcher.find()) {
                namePart = matcher.group(1);
                extPart1 = matcher.group(2);
                extPart2 = matcher.group(3);
            } else {
                throw new RuntimeException(
                        "Pattern " + ZONE_PATTERN + " not match '" + flinkSqlTypeUpperCase + "'");
            }
        } else {
            Matcher matcher = UNZONE_PATTERN.matcher(flinkSqlTypeUpperCase);
            if (matcher.find()) {
                namePart = matcher.group(1);
                extPart1 = matcher.group(2);
            } else {
                throw new RuntimeException(
                        "Pattern " + UNZONE_PATTERN + " not match '" + flinkSqlTypeUpperCase + "'");
            }
        }

        namePart = namePart.toUpperCase(Locale.ENGLISH);

        int javaSqlType;
        Integer precision = null;
        Integer scale = null;

        switch (namePart) {
            case "CHAR":
                javaSqlType = Types.CHAR;
                precision = parseIntOrElse(extPart1, 1);
                break;
            case "VARCHAR":
                javaSqlType = Types.VARCHAR;
                precision = parseIntOrElse(extPart1, 1);
                break;
            case "STRING":
                javaSqlType = Types.VARCHAR;
                precision = 2147483647;
                break;
            case "BOOLEAN":
                javaSqlType = Types.BOOLEAN;
                break;
            case "BINARY":
                javaSqlType = Types.BINARY;
                precision = parseIntOrElse(extPart1, 1);
                break;
            case "VARBINARY":
                javaSqlType = Types.VARBINARY;
                precision = parseIntOrElse(extPart1, 1);
                break;
            case "BYTES":
                javaSqlType = Types.VARBINARY;
                precision = 2147483647;
                break;
            case "DEC":
            case "NUMERIC":
            case "DECIMAL":
                javaSqlType = Types.DECIMAL;
                if (extPart1 == null) {
                    precision = 10;
                    scale = 0;
                } else if (extPart1.contains(",")) {
                    String[] splitPrecisionAndScale = extPart1.split(",");
                    precision = Integer.parseInt(splitPrecisionAndScale[0].trim());
                    scale = Integer.parseInt(splitPrecisionAndScale[1].trim());
                } else {
                    precision = Integer.parseInt(extPart1);
                }
                break;
            case "TINYINT":
                javaSqlType = Types.TINYINT;
                break;
            case "SMALLINT":
                javaSqlType = Types.SMALLINT;
                break;
            case "INT":
            case "INTEGER":
                javaSqlType = Types.INTEGER;
                break;
            case "BIGINT":
                javaSqlType = Types.BIGINT;
                break;
            case "FLOAT":
                javaSqlType = Types.FLOAT;
                break;
            case "DOUBLE":
            case "DOUBLE PRECISION":
                javaSqlType = Types.DOUBLE;
                break;
            case "DATE":
                javaSqlType = Types.DATE;
                break;
            case "TIME":
                javaSqlType = Types.TIME;
                precision = parseIntOrElse(extPart1, 0);
                break;
            case "TIMESTAMP":
                javaSqlType =
                        ("WITH LOCAL TIME ZONE".equals(extPart2)
                                        || "WITH TIME ZONE".equals(extPart2))
                                ? Types.TIMESTAMP_WITH_TIMEZONE
                                : Types.TIMESTAMP;
                precision = parseIntOrElse(extPart1, 6);
                break;
            case "TIMESTAMP_LTZ":
                javaSqlType = Types.TIMESTAMP_WITH_TIMEZONE;
                precision = parseIntOrElse(extPart1, 6);
                break;
            case "MULTISET":
            case "ARRAY":
                javaSqlType = Types.ARRAY;
                break;
            default:
                throw new RuntimeException("Unsupported " + namePart + " map to java.sql.Types");
        }
        return new Integer[] {javaSqlType, precision, scale};
    }

    private static int parseIntOrElse(String extPart1, int defaultValue) {
        return extPart1 == null ? defaultValue : Integer.parseInt(extPart1);
    }

    public static FlinkResultSet createTablesResultSet(
            Statement statement,
            List<String> schemas,
            Map<String, List<String>> schemaTables,
            Map<String, String[]> tablesInfo) {
        List<RowData> tableList = new ArrayList<>();
        List<String> schemaList = new ArrayList<>(schemas);
        schemaList.sort(String::compareTo);
        for (String schema : schemaList) {
            List<String> tables = schemaTables.get(schema);
            tables.sort(String::compareTo);
            tables.forEach(
                    t -> {
                        String[] tableInfo = tablesInfo.get(schema + "." + t);
                        tableList.add(
                                GenericRowData.of(
                                        StringData.fromString(tableInfo[0]),
                                        StringData.fromString(tableInfo[1]),
                                        StringData.fromString(tableInfo[2]),
                                        StringData.fromString(tableInfo[3]),
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null));
                    });
        }

        return new FlinkResultSet(
                statement,
                new CollectionResultIterator(tableList.iterator()),
                ResolvedSchema.of(
                        TABLE_CAT_COLUMN,
                        TABLE_SCHEM_COLUMN,
                        TABLE_NAME_COLUMN,
                        TABLE_TYPE_COLUMN,
                        REMARKS_COLUMN,
                        TYPE_CAT_COLUMN,
                        TYPE_SCHEM_COLUMN,
                        TYPE_NAME_COLUMN,
                        SELF_REFERENCING_COL_NAME_COLUMN,
                        REF_GENERATION_COLUMN));
    }

    public static FlinkResultSet createSchemasResultSet(Statement statement, List<String> schemas) {
        List<RowData> schemaList = new ArrayList<>();
        List<String> schemaNameList = new ArrayList<>(schemas);
        schemaNameList.sort(String::compareTo);
        for (String schema : schemaNameList) {
            schemaList.add(GenericRowData.of(StringData.fromString(schema)));
        }

        return new FlinkResultSet(
                statement,
                new CollectionResultIterator(schemaList.iterator()),
                ResolvedSchema.of(TABLE_SCHEM_COLUMN));
    }
}
