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

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Tests for. */
public class FlinkJDBCTest {

    public static void main(String[] args) {
        Pattern pattern =
                Pattern.compile(".*CONSTRAINT `([^`]+)` PRIMARY KEY \\(([^)]+)\\) NOT ENFORCED.*");
        Matcher matcher =
                pattern.matcher(
                        "CREATE TABLE `pai2`.`db_pai2`.`tb_columns` (\n"
                                + "  `f0` CHAR(1) NOT NULL COMMENT 'this is comment',\n"
                                + "  `f1` CHAR(1) NOT NULL,\n"
                                + "  `f2` CHAR(12) NOT NULL,\n"
                                + "  `f3` VARCHAR(2147483647),\n"
                                + "  `f4` VARCHAR(12),\n"
                                + "  `f5` VARCHAR(2147483647),\n"
                                + "  `f6` BOOLEAN,\n"
                                + "  `f7` BINARY(1),\n"
                                + "  `f8` VARBINARY(2147483647),\n"
                                + "  `f9` VARBINARY(12),\n"
                                + "  `f10` VARBINARY(2147483647),\n"
                                + "  `f11` DECIMAL(10, 0),\n"
                                + "  `f12` DECIMAL(10, 0),\n"
                                + "  `f13` DECIMAL(10, 0),\n"
                                + "  `f14` DECIMAL(11, 2),\n"
                                + "  `f15` TINYINT,\n"
                                + "  `f16` SMALLINT,\n"
                                + "  `f17` INT,\n"
                                + "  `f18` INT,\n"
                                + "  `f19` BIGINT,\n"
                                + "  `f20` FLOAT,\n"
                                + "  `f21` DOUBLE,\n"
                                + "  `f22` DOUBLE,\n"
                                + "  `f23` DATE,\n"
                                + "  `f24` TIME(0),\n"
                                + "  `f25` TIME(0),\n"
                                + "  `f26` TIMESTAMP(6),\n"
                                + "  `f27` TIMESTAMP(3),\n"
                                + "  `f28` TIMESTAMP(6) WITH LOCAL TIME ZONE,\n"
                                + "  `f29` TIMESTAMP(3) WITH LOCAL TIME ZONE,\n"
                                + "  `f30` BIGINT,\n"
                                + "  CONSTRAINT `PK_f0_f1_f2` PRIMARY KEY (`f0`, `f1`, `f2`) NOT ENFORCED\n"
                                + ") COMMENT '1234'\n"
                                + "WITH (\n"
                                + "  'path' = '/paimon_warehouse/pai02/db_pai2.db/tb_columns'\n"
                                + ")");
        if (!matcher.find()) {
            System.out.println("no find");
        } else {
            String name = matcher.group(1);
            String len = matcher.group(2);
            System.out.println(name);
            System.out.println(len);
        }
    }

    @Test
    public void test1() throws Exception {
        try (Connection connection =
                DriverManager.getConnection("jdbc:flink://tools.k8s.io:8083/default_catalog")) {
            System.out.println("Catalogs ==========");
            resultSetToListAndClose(connection.getMetaData().getCatalogs())
                    .forEach(System.out::println);
            System.out.println("Current ==========");
            System.out.println("current catalog: " + connection.getCatalog());
            System.out.println("current schema: " + connection.getSchema());
            System.out.println("Schemas ==========");
            resultSetToListAndClose(connection.getMetaData().getSchemas())
                    .forEach(System.out::println);
        }
    }

    private List<String> resultSetToListAndClose(ResultSet resultSet) throws Exception {
        List<String> resultList = new ArrayList<>();
        int columnCount = resultSet.getMetaData().getColumnCount();
        while (resultSet.next()) {
            List<String> columnStringList = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                columnStringList.add(resultSet.getString(i));
            }
            resultList.add(StringUtils.join(columnStringList, ","));
        }
        resultSet.close();

        return resultList;
    }
}
