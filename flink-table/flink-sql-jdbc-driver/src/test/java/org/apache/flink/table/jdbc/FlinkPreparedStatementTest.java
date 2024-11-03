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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for flink statement. */
public class FlinkPreparedStatementTest extends FlinkJdbcDriverTestBase {
    @TempDir private Path tempDir;

    @Test
    @Timeout(value = 60)
    public void testExecuteQuery() throws Exception {
        try (FlinkConnection connection = new FlinkConnection(getDriverUri())) {
            try (PreparedStatement statement =
                    connection.prepareStatement(
                            String.format(
                                    "CREATE TABLE test_table("
                                            + "st string, "
                                            + "bo boolean, "
                                            + "ti tinyint, "
                                            + "sm smallint, "
                                            + "_in int, "
                                            + "bi bigint, "
                                            + "fl float, "
                                            + "do double, "
                                            + "da date,"
                                            + "tim time,"
                                            + "tims TIMESTAMP"
                                            + ") with ("
                                            + "'connector'='filesystem',\n"
                                            + "'format'='csv',\n"
                                            + "'path'='%s')",
                                    tempDir))) {
                // CREATE TABLE is not a query and has no results
                assertFalse(statement.execute());
                assertEquals(0, statement.getUpdateCount());
            }

            // INSERT TABLE returns job id
            try (PreparedStatement statement =
                    connection.prepareStatement(
                            "INSERT INTO test_table VALUES (?,?,?,?,?,?,?,?,?,?,?)")) {
                statement.setString(1, "abc");
                statement.setBoolean(2, false);
                statement.setByte(3, Integer.valueOf(1).byteValue());
                statement.setShort(4, Integer.valueOf(2).shortValue());
                statement.setInt(5, 3);
                statement.setLong(6, 4L);
                statement.setFloat(7, 5.5F);
                statement.setDouble(8, 6.6);
                statement.setDate(9, new Date(10L));
                statement.setTime(10, new Time(1000L));
                statement.setTimestamp(11, new Timestamp(1730541439000L));
                statement.execute();

                assertThatThrownBy(statement::getUpdateCount)
                        .isInstanceOf(SQLFeatureNotSupportedException.class)
                        .hasMessage("FlinkStatement#getUpdateCount is not supported for query");

                String jobId;
                try (ResultSet resultSet = statement.getResultSet()) {
                    assertTrue(resultSet.next());
                    assertEquals(1, resultSet.getMetaData().getColumnCount());
                    jobId = resultSet.getString("job id");
                    assertEquals(jobId, resultSet.getString(1));
                    assertFalse(resultSet.next());
                }
                assertNotNull(jobId);
                // Wait job finished
                boolean jobFinished = false;
                while (!jobFinished) {
                    assertTrue(statement.execute("SHOW JOBS"));
                    try (ResultSet resultSet = statement.getResultSet()) {
                        while (resultSet.next()) {
                            if (resultSet.getString(1).equals(jobId)) {
                                if (resultSet.getString(3).equals("FINISHED")) {
                                    jobFinished = true;
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            // SELECT all data from test_table
            try (PreparedStatement statement =
                    connection.prepareStatement("SELECT * FROM test_table")) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    assertEquals(11, resultSet.getMetaData().getColumnCount());
                    List<String> resultList = new ArrayList<>();
                    while (resultSet.next()) {
                        assertEquals(resultSet.getString("st"), resultSet.getString(1));
                        assertEquals(resultSet.getBoolean("bo"), resultSet.getBoolean(2));
                        assertEquals(resultSet.getByte("ti"), resultSet.getByte(3));
                        assertEquals(resultSet.getShort("sm"), resultSet.getShort(4));
                        assertEquals(resultSet.getInt("_in"), resultSet.getInt(5));
                        assertEquals(resultSet.getLong("bi"), resultSet.getLong(6));
                        assertEquals(resultSet.getFloat("fl"), resultSet.getFloat(7));
                        assertEquals(resultSet.getDouble("do"), resultSet.getDouble(8));
                        assertEquals(resultSet.getDate("da"), resultSet.getDate(9));
                        assertEquals(resultSet.getTime("tim"), resultSet.getTime(10));
                        assertEquals(resultSet.getTimestamp("tims"), resultSet.getTimestamp(11));
                        resultList.add(
                                String.format(
                                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                                        resultSet.getString("st"),
                                        resultSet.getBoolean("bo"),
                                        resultSet.getByte("ti"),
                                        resultSet.getShort("sm"),
                                        resultSet.getInt("_in"),
                                        resultSet.getLong("bi"),
                                        resultSet.getFloat("fl"),
                                        resultSet.getDouble("do"),
                                        resultSet.getDate("da"),
                                        resultSet.getTime("tim"),
                                        resultSet.getTimestamp("tims")));
                    }
                    assertThat(resultList)
                            .containsExactlyInAnyOrder(
                                    "abc,false,1,2,3,4,5.5,6.6,1970-01-01,16:00:01,2024-11-02 17:57:19.0");
                }

                assertTrue(statement.execute("SHOW JOBS"));
                try (ResultSet resultSet = statement.getResultSet()) {
                    // Check there are two finished jobs.
                    int count = 0;
                    while (resultSet.next()) {
                        assertEquals("FINISHED", resultSet.getString(3));
                        count++;
                    }
                    assertEquals(2, count);
                }
            }
        }
    }

    @Test
    public void testCloseAllStatements() throws Exception {
        FlinkConnection connection = new FlinkConnection(getDriverUri());
        PreparedStatement statement1 = connection.prepareStatement("select 1");
        PreparedStatement statement2 = connection.prepareStatement("select 1");
        PreparedStatement statement3 = connection.prepareStatement("select 1");

        statement1.close();
        connection.close();

        assertTrue(statement1.isClosed());
        assertTrue(statement2.isClosed());
        assertTrue(statement3.isClosed());
    }

    @Test
    public void testCloseNonQuery() throws Exception {
        CompletableFuture<Void> closedFuture = new CompletableFuture<>();
        try (FlinkConnection connection = new FlinkConnection(new TestingExecutor(closedFuture))) {
            try (PreparedStatement statement = connection.prepareStatement("INSERT")) {
                assertThatThrownBy(statement::executeQuery)
                        .hasMessage(String.format("Statement[%s] is not a query.", "INSERT"));
                closedFuture.get(10, TimeUnit.SECONDS);
            }
        }
    }

    /** Testing executor. */
    static class TestingExecutor implements Executor {
        private final CompletableFuture<Void> closedFuture;

        TestingExecutor(CompletableFuture<Void> closedFuture) {
            this.closedFuture = closedFuture;
        }

        @Override
        public void configureSession(String statement) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReadableConfig getSessionConfig() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> getSessionConfigMap() {
            throw new UnsupportedOperationException();
        }

        @Override
        public StatementResult executeStatement(String statement) {
            return new StatementResult(
                    null,
                    new CloseableIterator<RowData>() {
                        @Override
                        public void close() throws Exception {
                            closedFuture.complete(null);
                        }

                        @Override
                        public boolean hasNext() {
                            return false;
                        }

                        @Override
                        public RowData next() {
                            throw new UnsupportedOperationException();
                        }
                    },
                    false,
                    ResultKind.SUCCESS_WITH_CONTENT,
                    JobID.generate());
        }

        @Override
        public List<String> completeStatement(String statement, int position) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }
}
