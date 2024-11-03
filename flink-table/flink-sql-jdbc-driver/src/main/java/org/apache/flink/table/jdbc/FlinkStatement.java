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

import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.jdbc.utils.CollectionResultIterator;

import javax.annotation.concurrent.NotThreadSafe;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collections;

/** Statement for flink jdbc driver. Notice that the statement is not thread safe. */
@NotThreadSafe
public class FlinkStatement extends BaseStatement {
    private final FlinkConnection connection;
    private final Executor executor;
    private FlinkResultSet currentResults;
    private boolean hasResults;
    private boolean closed;

    public FlinkStatement(FlinkConnection connection) {
        this.connection = connection;
        this.executor = connection.getExecutor();
        this.currentResults = null;
    }

    /**
     * Execute a SELECT query.
     *
     * @param sql an SQL statement to be sent to the database, typically a static SQL <code>SELECT
     *     </code> statement
     * @return the select query result set.
     * @throws SQLException the thrown exception
     */
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        StatementResult result = executeInternal(sql);
        if (!result.isQueryResult()) {
            result.close();
            throw new SQLException(String.format("Statement[%s] is not a query.", sql));
        }
        currentResults = new FlinkResultSet(this, result);
        hasResults = true;

        return currentResults;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        try (StatementResult result = executeInternal(sql)) {
            return 0;
        }
    }

    private void clearCurrentResults() throws SQLException {
        if (currentResults == null) {
            return;
        }
        currentResults.close();
        currentResults = null;
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }

        cancel();
        connection.removeStatement(this);
        closed = true;
    }

    @Override
    public void cancel() throws SQLException {
        checkClosed();
        clearCurrentResults();
    }

    // TODO We currently do not support this, but we can't throw a SQLException here because we want
    // to support jdbc tools such as beeline and sqlline.
    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    // TODO We currently do not support this, but we can't throw a SQLException here because we want
    // to support jdbc tools such as beeline and sqlline.
    @Override
    public void clearWarnings() throws SQLException {}

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("This result set is already closed");
        }
    }

    /**
     * Execute a sql statement. Notice that the <code>INSERT</code> statement in Flink would return
     * job id as result set.
     *
     * @param sql any SQL statement
     * @return True if there is result set for the statement.
     * @throws SQLException the thrown exception.
     */
    @Override
    public boolean execute(String sql) throws SQLException {
        StatementResult result = executeInternal(sql);
        if (result.isQueryResult() || result.getResultKind() == ResultKind.SUCCESS_WITH_CONTENT) {
            currentResults = new FlinkResultSet(this, result);
            hasResults = true;
            return true;
        }

        hasResults = false;
        return false;
    }

    private StatementResult executeInternal(String sql) throws SQLException {
        checkClosed();
        clearCurrentResults();
        try {
            return executor.executeStatement(sql);
        } catch (Exception e2) {
            String stackTrace = e2.getCause().getMessage();
            int lines = 0;
            for (int i = 0; i < stackTrace.length(); i++) {
                if ('\n' == stackTrace.charAt(i)) {
                    lines++;
                }
            }
            // if StackTrace more than 5 lines, get root cause
            if (lines > 5) {
                final String causeCaption = "Caused by: ";
                String rootCause = null;
                String[] split = stackTrace.split("\\n");
                for (int i = split.length - 1; i >= 0; i--) {
                    if (split[i].startsWith(causeCaption)) {
                        rootCause = split[i].replace(causeCaption, "").trim();
                        if (rootCause.contains(":")) {
                            rootCause = rootCause.substring(rootCause.indexOf(":") + 1).trim();
                        }
                        break;
                    }
                }
                throw new SQLException(rootCause);
            }
            throw new SQLException(stackTrace);
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();

        if (currentResults == null) {
            throw new SQLException("No result set in the current statement.");
        }
        if (currentResults.isClosed()) {
            throw new SQLException("Result set has been closed");
        }

        if (connection.isWaitJobTerminalState()
                && currentResults.getMetaData().getColumnCount() == 1
                && "job id".equals(currentResults.getMetaData().getColumnName(1))) {
            Statement statement = currentResults.getStatement();
            currentResults.next();
            String jobId = currentResults.getString("job id");
            RowData jobStateRowData = null;
            ResolvedSchema jobSchema = null;
            boolean jobRunning = true;
            while (jobRunning) {
                try (StatementResult statementResult = executeInternal("SHOW JOBS")) {
                    jobSchema = statementResult.getResultSchema();
                    while (statementResult.hasNext()) {
                        RowData rowData = statementResult.next();
                        if (jobId.equals(rowData.getString(0).toString())) {
                            jobStateRowData = rowData;
                            if ("FAILED".equals(rowData.getString(2).toString())) {
                                String errorMessage;
                                if (rowData.getArity() < 4) {
                                    errorMessage =
                                            "job was failed, but gateway no response error message";
                                } else {
                                    errorMessage = rowData.getString(4).toString();
                                }
                                throw new SQLException(
                                        String.format("(%s) %s", jobId, errorMessage));
                            }
                            if ("FINISHED".equals(rowData.getString(2).toString())) {
                                jobRunning = false;
                            }
                        }
                    }
                }
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            return new FlinkResultSet(
                    statement,
                    new CollectionResultIterator(Collections.singleton(jobStateRowData).iterator()),
                    jobSchema);
        }
        return currentResults;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkClosed();

        if (currentResults != null) {
            cancel();
            return false;
        }

        throw new SQLFeatureNotSupportedException("Multiple open results not supported");
    }

    @Override
    public int getUpdateCount() throws SQLException {
        if (hasResults) {
            throw new SQLFeatureNotSupportedException(
                    "FlinkStatement#getUpdateCount is not supported for query");
        } else {
            return 0;
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }
}
