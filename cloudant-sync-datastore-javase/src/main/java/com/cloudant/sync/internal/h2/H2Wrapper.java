package com.cloudant.sync.internal.h2;

import com.cloudant.sync.internal.android.ContentValues;
import com.cloudant.sync.internal.sqlite.Cursor;
import com.cloudant.sync.internal.sqlite.SQLDatabase;
import com.cloudant.sync.internal.sqlite.sqlite4java.QueryBuilder;
import com.cloudant.sync.internal.sqlite.sqlite4java.SQLiteCursor;
import com.cloudant.sync.internal.sqlite.sqlite4java.Tuple;

import java.io.File;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Created by tomblench on 28/11/2016.
 */

public class H2Wrapper extends SQLDatabase {

    private File dbFile;
    private Connection dbConnection;

    public H2Wrapper(File dbFile) {
        this.dbFile = dbFile;
    }

    public static H2Wrapper open(File dbFile) {
        H2Wrapper h2 = new H2Wrapper(dbFile);
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
            Class.forName("org.h2.Driver");
            String path = String.format(Locale.ENGLISH, "jdbc:h2:%s", dbFile.getCanonicalPath());
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

    @Override
    public void beginTransaction() {

    }

    @Override
    public void endTransaction() {

    }

    @Override
    public void setTransactionSuccessful() {

    }

    @Override
    public int update(String table, ContentValues values, String whereClause, String[] whereArgs) {
        try {
            String updateQuery = QueryBuilder.buildUpdateQuery(table, values, whereClause, whereArgs);
            Object[] bindArgs = QueryBuilder.buildBindArguments(values, whereArgs);
            PreparedStatement ps = dbConnection.prepareStatement(updateQuery);
            bindArguments(ps, bindArgs);
            ps.execute();
            return ps.getUpdateCount();
        } catch (SQLException sqe) {
            // TODO
            throw new RuntimeException(sqe);
        }
    }

    @Override
    public Cursor rawQuery(String sql, String[] selectionArgs) throws SQLException {
        PreparedStatement ps = dbConnection.prepareStatement(sql);
        bindArguments(ps, selectionArgs);
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData rsmd = rs.getMetaData();
        // get types and names
        ArrayList<Integer> types = new ArrayList<Integer>();
        ArrayList<String> names = new ArrayList<String>();
        for (int i=0; i<rsmd.getColumnCount(); i++) {
            types.add(i, rsmd.getColumnType(i));
            names.add(i, rsmd.getColumnName(i));
        }
        List<Tuple> resultSet = new ArrayList<Tuple>();
        while (rs.next()) {
            Tuple t = new Tuple(types);
            for (int i=0; i<rsmd.getColumnCount(); i++) {
                switch (rsmd.getColumnType(i)) {
                    case Types.VARCHAR:
                        t.put(i, rs.getString(i));
                        break;
                    case Types.INTEGER:
                        t.put(i, rs.getInt(i));
                        break;
                    case Types.FLOAT:
                        t.put(i, rs.getFloat(i));
                        break;
                    case Types.DOUBLE:
                        // TODO - naughty cast
                        t.put(i, (float)rs.getDouble(i));
                        break;
                    case Types.BLOB:
                        Blob b = rs.getBlob(i);
                        // TODO - naughty cast
                        byte[] bytes = b.getBytes(0, (int)b.length());
                        t.put(i, bytes);
                    default:
                        throw new RuntimeException("Don't know about type "+rsmd.getColumnType(i));
                }
            }
            resultSet.add(t);
        }
        return new SQLiteCursor(names, resultSet);
    }

    @Override
    public int delete(String table, String whereClause, String[] whereArgs) {
        return 0;
    }

    @Override
    public long insert(String table, ContentValues values) {
        return 0;
    }

    @Override
    public long insertWithOnConflict(String table, ContentValues initialValues, int conflictAlgorithm) {
        return 0;
    }

    private static void bindArguments(PreparedStatement ps, Object[] bindArgs) throws SQLException {
        int i = 0;
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
            else {
                throw new RuntimeException("Don't know type of "+o);
            }
        }
    }
}
