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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;

/** FlinkPreparedStatement. */
public class FlinkPreparedStatement extends FlinkStatement implements PreparedStatement {
    private final String sql;

    /** save the SQL parameters {paramLoc:paramValue}. */
    private final HashMap<Integer, String> parameters = new HashMap<>();

    public FlinkPreparedStatement(FlinkConnection connection, String sql) {
        super(connection);
        this.sql = sql;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return super.executeQuery(updateSql(sql, parameters));
    }

    @Override
    public int executeUpdate() throws SQLException {
        return super.executeUpdate(updateSql(sql, parameters));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        // TODO use CASE(NULL as xxx) to set null
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        this.parameters.put(parameterIndex, "" + x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        this.parameters.put(parameterIndex, String.format("cast(%s as tinyint)", x));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        this.parameters.put(parameterIndex, String.format("cast(%s as smallint)", x));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        this.parameters.put(parameterIndex, "" + x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        this.parameters.put(parameterIndex, "" + x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        this.parameters.put(parameterIndex, "" + x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        this.parameters.put(parameterIndex, "" + x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        if (x == null) {
            this.parameters.put(parameterIndex, "cast(null as decimal)");
        } else {
            this.parameters.put(parameterIndex, x.toString());
        }
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        if (x == null) {
            this.parameters.put(parameterIndex, "cast(null as string)");
        } else {
            x = replaceBackSlashSingleQuote(x);
            x = x.replace("'", "\\'");
            this.parameters.put(parameterIndex, "'" + x + "'");
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        if (x == null) {
            this.parameters.put(parameterIndex, "cast(null as date)");
        } else {
            this.parameters.put(parameterIndex, "date '" + x + "'");
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        if (x == null) {
            this.parameters.put(parameterIndex, "cast(null as time)");
        } else {
            this.parameters.put(parameterIndex, "time '" + x + "'");
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        if (x == null) {
            this.parameters.put(parameterIndex, "cast(null as timestamp)");
        } else {
            this.parameters.put(parameterIndex, "timestamp '" + x + "'");
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void clearParameters() throws SQLException {
        this.parameters.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x == null) {
            throw new SQLFeatureNotSupportedException("Method not supported when object is null");
        } else if (x instanceof String) {
            setString(parameterIndex, (String) x);
        } else if (x instanceof Short) {
            setShort(parameterIndex, (Short) x);
        } else if (x instanceof Integer) {
            setInt(parameterIndex, (Integer) x);
        } else if (x instanceof Long) {
            setLong(parameterIndex, (Long) x);
        } else if (x instanceof Float) {
            setFloat(parameterIndex, (Float) x);
        } else if (x instanceof Double) {
            setDouble(parameterIndex, (Double) x);
        } else if (x instanceof Boolean) {
            setBoolean(parameterIndex, (Boolean) x);
        } else if (x instanceof Byte) {
            setByte(parameterIndex, (Byte) x);
        } else if (x instanceof Character) {
            setString(parameterIndex, x.toString());
        } else if (x instanceof Timestamp) {
            setTimestamp(parameterIndex, (Timestamp) x);
        } else if (x instanceof BigDecimal) {
            setBigDecimal(parameterIndex, (BigDecimal) x);
        } else {
            // Can't infer a type.
            throw new SQLException(
                    MessageFormat.format(
                            "Can''t infer the SQL type to use for an instance of {0}. Use setObject() with an explicit Types value to specify the type to use.",
                            x.getClass().getName()));
        }
    }

    @Override
    public boolean execute() throws SQLException {
        return super.execute(updateSql(sql, parameters));
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        if (typeName == null) {
            throw new SQLFeatureNotSupportedException("Method not supported when typeName is None");
        }
        this.parameters.put(parameterIndex, String.format("CAST(NULL AS %s)", typeName));
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        this.setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    private String replaceBackSlashSingleQuote(String x) {
        // scrutinize escape pair, specifically, replace \' to '
        StringBuilder newX = new StringBuilder();
        for (int i = 0; i < x.length(); i++) {
            char c = x.charAt(i);
            if (c == '\\' && i < x.length() - 1) {
                char c1 = x.charAt(i + 1);
                if (c1 == '\'') {
                    newX.append(c1);
                } else {
                    newX.append(c);
                    newX.append(c1);
                }
                i++;
            } else {
                newX.append(c);
            }
        }
        return newX.toString();
    }

    private String updateSql(final String sql, HashMap<Integer, String> parameters)
            throws SQLException {
        List<String> parts = splitSqlStatement(sql);

        StringBuilder newSql = new StringBuilder(parts.get(0));
        for (int i = 1; i < parts.size(); i++) {
            if (!parameters.containsKey(i)) {
                throw new SQLException("Parameter #" + i + " is unset");
            }
            newSql.append(parameters.get(i));
            newSql.append(parts.get(i));
        }
        return newSql.toString();
    }

    /**
     * Splits the parametered sql statement at parameter boundaries.
     *
     * @param sql 'select 1 from ? where a = ?'
     * @return ['select 1 from ',' where a = ',''].
     */
    private List<String> splitSqlStatement(String sql) {
        List<String> parts = new ArrayList<>();
        int apCount = 0;
        int off = 0;
        boolean skip = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (skip) {
                skip = false;
                continue;
            }
            switch (c) {
                case '\'':
                    apCount++;
                    break;
                case '\\':
                    skip = true;
                    break;
                case '?':
                    if ((apCount & 1) == 0) {
                        parts.add(sql.substring(off, i));
                        off = i + 1;
                    }
                    break;
                default:
                    break;
            }
        }
        parts.add(sql.substring(off));
        return parts;
    }
}
