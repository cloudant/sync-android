package com.cloudant.sync.internal.hypersql;

import com.cloudant.sync.internal.android.ContentValues;
import com.cloudant.sync.internal.sqlite.Cursor;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.sqlite.sqlite4java.QueryBuilder;
import com.cloudant.sync.internal.sqlite.sqlite4java.SQLiteCursor;
import com.cloudant.sync.internal.sqlite.sqlite4java.Tuple;
import com.cloudant.sync.internal.util.Misc;

import java.io.File;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Created by tomblench on 28/11/2016.
 */

public class HyperSqlWrapper extends SQLDatabase {

    private File dbFile;
    private Connection dbConnection;

    public HyperSqlWrapper(File dbFile) {
        this.dbFile = dbFile;
    }

    public static HyperSqlWrapper open(File dbFile) {
        HyperSqlWrapper h2 = new HyperSqlWrapper(dbFile);
        h2.open();
        return h2;
    }

    @Override
    public void execSQL(String sql, Object[] bindArgs) throws SQLException {
        PreparedStatement ps = dbConnection.prepareStatement(sql);
        bindArguments(ps, bindArgs);
        ps.execute();
    }

    @Override
    public void execSQL(String sql) throws SQLException {
        PreparedStatement ps = dbConnection.prepareStatement(sql);
        ps.execute();
    }

    @Override
    public void compactDatabase() {

    }

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public void open() {
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
            String path = String.format(Locale.ENGLISH, "jdbc:hsqldb:file:%s", dbFile.getCanonicalPath());
            dbConnection = DriverManager.
                    getConnection(path, "sa", "");
        } catch (Exception e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            if (dbConnection != null) {
                dbConnection.close();
            }
        } catch (SQLException sqe) {
            // TODO
            throw new RuntimeException(sqe);
        }
    }

    @Override
    public boolean isOpen() {
        try {
            return !dbConnection.isClosed();
        } catch (SQLException sqe) {
            // TODO
            throw new RuntimeException(sqe);
        }
    }

    boolean transactionSuccessful;

    @Override
    public void beginTransaction() {
        try {
            PreparedStatement ps = dbConnection.prepareStatement("START TRANSACTION; ");
            ps.execute();
            this.transactionSuccessful = false;
        } catch (SQLException sqe) {
            throw new RuntimeException(sqe);
        }


    }

    @Override
    public void endTransaction() {
        try {
            if (this.transactionSuccessful) {
                PreparedStatement ps = dbConnection.prepareStatement("COMMIT; ");
                ps.execute();
            } else {
                PreparedStatement ps = dbConnection.prepareStatement("ROLLBACK; ");
                ps.execute();

            }
        } catch (SQLException sqe) {
            throw new RuntimeException(sqe);
        }

    }

    @Override
    public void setTransactionSuccessful() {
        this.transactionSuccessful = true;

    }

    @Override
    public int update(String table, ContentValues values, String whereClause, String[] whereArgs) {
        try {
            String updateQuery = QueryBuilder.buildUpdateQuery(table, values, whereClause, whereArgs);
            Object[] bindArgs = QueryBuilder.buildBindArguments(values, whereArgs);
            PreparedStatement ps = dbConnection.prepareStatement(updateQuery);
            bindArguments(ps, bindArgs);
            ps.execute();
            ps.getUpdateCount();

            return getRowId();

        } catch (SQLException sqe) {
            // TODO
            throw new RuntimeException(sqe);
        }
    }

    @Override
    public Cursor rawQuery(String sql, String[] selectionArgs) throws SQLException {

        System.out.println("rawquery "+sql+", "+ Arrays.toString(selectionArgs));

        PreparedStatement ps = dbConnection.prepareStatement(sql);
        bindArguments(ps, selectionArgs);
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData rsmd = rs.getMetaData();
        // get types and names
        ArrayList<Integer> types = new ArrayList<Integer>();
        ArrayList<String> names = new ArrayList<String>();
        for (int i=0; i<rsmd.getColumnCount(); i++) {
            // map between jdbc types and our types
            int convertedType;
            switch (rsmd.getColumnType(i+1)) {
                case Types.VARCHAR:
                    convertedType = Cursor.FIELD_TYPE_STRING;
                    break;
                case Types.INTEGER:
                case Types.BIGINT:
                    convertedType = Cursor.FIELD_TYPE_INTEGER;
                    break;
                case Types.FLOAT:
                case Types.DOUBLE:
                    convertedType = Cursor.FIELD_TYPE_FLOAT;
                    break;
                case Types.BLOB:
                    convertedType = Cursor.FIELD_TYPE_BLOB;
                    break;
                case Types.BOOLEAN:
                    convertedType = Cursor.FIELD_TYPE_INTEGER;
                    break;
                default:
                    throw new RuntimeException("Don't know about type "+rsmd.getColumnType(i+1));
            }
            types.add(i, convertedType);
            names.add(i, rsmd.getColumnName(i+1));
        }
        List<Tuple> resultSet = new ArrayList<Tuple>();
        while (rs.next()) {
            Tuple t = new Tuple(types);
            for (int i=0; i<rsmd.getColumnCount(); i++) {
                switch (rsmd.getColumnType(i+1)) {
                    case Types.VARCHAR:
                        t.put(i, rs.getString(i+1));
                        break;
                    case Types.INTEGER:
                        int integer = rs.getInt(i + 1);
                        if (rs.wasNull()) {
                            // hacky
                            t.put(i, -1);
                        } else {
                            t.put(i, integer);
                        }
                        break;
                    case Types.BIGINT:
                        long theLong = rs.getLong(i + 1);
                        if (rs.wasNull()) {
                            // hacky
                            t.put(i, -1);
                        } else {
                            t.put(i, theLong);
                        }
                        break;
                    case Types.FLOAT:
                        t.put(i, rs.getFloat(i+1));
                        break;
                    case Types.DOUBLE:
                        // TODO - naughty cast
                        t.put(i, (float)rs.getDouble(i+1));
                        break;
                    case Types.BLOB:
                        Blob b = rs.getBlob(i+1);
                        // TODO - naughty cast
                        byte[] bytes = b.getBytes(1, (int)b.length());
                        t.put(i, bytes);
                        break;
                    case Types.BOOLEAN:
                        t.put(i, rs.getBoolean(i+1) ? 1 : 0);
                        break;
                    default:
                        throw new RuntimeException("Don't know about type "+rsmd.getColumnType(i+1));
                }
            }
            resultSet.add(t);
        }
        return new SQLiteCursor(names, resultSet);
    }

    @Override
    public int delete(String table, String whereClause, String[] whereArgs) {
        try {
            String sql = String.format("DELETE FROM %s %s",
                    table,
                    !Misc.isStringNullOrEmpty(whereClause) ?
                            String.format("WHERE %s", whereClause) : "");
            PreparedStatement ps = dbConnection.prepareStatement(sql);
            bindArguments(ps, whereArgs);
            int nRows = ps.executeUpdate();

            return getRowId();
        } catch (SQLException sqe) {
            throw new RuntimeException(sqe);
        }

    }

    @Override
    public long insert(String table, ContentValues values) {

        try {
            List<String> colNames = new ArrayList<String>();
            List<String> placeholders = new ArrayList<String>();
            Object[] bindArgs = new Object[values.size()];
            int i=0;
            for (String colName : values.keySet()) {
                colNames.add(colName);
                placeholders.add("?");
                bindArgs[i++] = values.get(colName);
            }

            String sql = String.format("INSERT INTO %s (%s) VALUES (%s)",
                    table,
                    Misc.join(",", colNames),
                    Misc.join(",", placeholders)
            );
            System.out.println(sql);
            System.out.println(values);

            PreparedStatement ps = dbConnection.prepareStatement(sql);
            bindArguments(ps, bindArgs);
            int nRows = ps.executeUpdate();

            return getRowId();
        } catch (SQLException sqe) {
            throw new RuntimeException(sqe);
        }
    }

    @Override
    public long insertWithOnConflict(String table, ContentValues initialValues, int conflictAlgorithm) {

            return insert(table, initialValues);
    }

    private static void bindArguments(PreparedStatement ps, Object[] bindArgs) throws SQLException {
        if (bindArgs == null || bindArgs.length == 0) {
            return;
        }
        int i = 1;
        for (Object o : bindArgs) {
            if (o instanceof String) {
                ps.setString(i++, (String)o);
            }
            else if (o instanceof Long) {
                ps.setLong(i++, (Long)o);
            }
            else if (o instanceof Integer) {
                ps.setInt(i++, (Integer)o);
            }
            else if (o instanceof Float) {
                ps.setFloat(i++, (Float)o);
            }
            else if (o instanceof Double) {
                ps.setDouble(i++, (Double)o);
            }
            else if (o instanceof Boolean) {
                ps.setBoolean(i++, (Boolean)o);
            }
            else if (o instanceof byte[]) {
                ps.setBytes(i++, (byte[])o);
            }
            else if (o == null) {
                int type = ps.getParameterMetaData().getParameterType(i);
                ps.setNull(i++, type);
            }
            else {
                throw new RuntimeException("Don't know type of "+o+", "+o.getClass());
            }
        }
    }

    private int getRowId() throws SQLException {
        CallableStatement getRowId = dbConnection.prepareCall("CALL IDENTITY(); ");
        //getRowId.registerOutParameter(1, Types.INTEGER);
        getRowId.execute();
        ResultSet rs = getRowId.getResultSet();
        boolean ok = rs.next();
        int rowId = rs.getInt(1);
        System.out.println("rowid "+rowId);
        return rowId;
    }

}
