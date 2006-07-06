package com.aoindustries.aoserv.master;

/*
 * Copyright 2003-2006 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.email.*;
import com.aoindustries.profiler.*;
import com.aoindustries.sql.*;
import com.aoindustries.util.*;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * @author  AO Industries, Inc.
 *
 * TODO: Might use CronDaemon and schedule based on each table update frequency versus table size.
 */
public final class BackupDatabaseSynchronizer implements Runnable {

    /**
     * The maximum time for a synchronization.
     */
    private static final long TIMER_MAX_TIME=20L*60*1000;

    /**
     * The interval in which the administrators will be reminded.
     */
    private static final long TIMER_REMINDER_INTERVAL=6L*60*60*1000;

    /**
     * The list of tables that are synchronized.
     */
    private static final int[] tableIDs={
        SchemaTable.AO_SERVERS,
        SchemaTable.BACKUP_LEVELS,
        SchemaTable.BACKUP_RETENTIONS,
        SchemaTable.BUSINESS_SERVERS,
        SchemaTable.BUSINESSES,
        SchemaTable.MASTER_SERVERS,
        SchemaTable.MYSQL_SERVERS,
        SchemaTable.PACKAGES,
        SchemaTable.POSTGRES_SERVERS,
        SchemaTable.SERVER_FARMS,
        SchemaTable.SERVERS,
        SchemaTable.USERNAMES
    };
    
    /**
     * The list of primary key columns.
     */
    private static final String[] primaryKeyColumns={
        "server", // AO_SERVERS
        "level", // BACKUP_LEVELS
        "days", // BACKUP_RETENTIONS
        "pkey", // BUSINESS_SERVERS
        "accounting", // BUSINESSES
        "pkey", // MASTER_SERVERS
        "pkey", // MYSQL_SERVERS
        "pkey", // PACKAGES
        "pkey", // POSTGRES_SERVERS
        "name", // SERVER_FARMS
        "pkey", // SERVERS
        "username" // USERNAMES
    };

    /**
     * The list of columns.
     */
    private static final String[][] columns={
        {"server", "failover_server"}, // AO_SERVERS
        {"level"}, // BACKUP_LEVELS
        {"days"}, // BACKUP_RETENTIONS
        {"pkey", "accounting", "server"}, // BUSINESS_SERVERS
        {"accounting", "canceled", "parent"}, // BUSINESSES
        {"pkey", "username", "server"}, // MASTER_SERVERS
        {"pkey", "name", "ao_server"}, // MYSQL_SERVERS
        {"pkey", "name", "accounting"}, // PACKAGES
        {"pkey", "name", "ao_server"}, // POSTGRES_SERVERS
        {"name", "allow_same_server_backup", "backup_farm"}, // SERVER_FARMS
        {"pkey", "hostname", "farm"}, // SERVERS
        {"username", "package"} // USERNAMES
    };
    
    /**
     * The list of column types.
     */
    private static final Class[][] columnTypes={
        {Integer.class, Integer.class}, // AO_SERVERS
        {Short.class}, // BACKUP_LEVELS
        {Short.class}, // BACKUP_RETENTIONS
        {Integer.class, String.class, Integer.class}, // BUSINESS_SERVERS
        {String.class, Timestamp.class, String.class}, // BUSINESSES
        {Integer.class, String.class, Integer.class}, // MASTER_SERVERS
        {Integer.class, String.class, Integer.class}, // MYSQL_SERVERS
        {Integer.class, String.class, String.class}, // PACKAGES
        {Integer.class, String.class, Integer.class}, // POSTGRES_SERVERS
        {String.class, Boolean.class, String.class}, // SERVER_FARMS
        {Integer.class, String.class, String.class}, // SERVERS
        {String.class, String.class} // USERNAMES
    };

    /**
     * The threads that are running.
     */
    private static final Thread[] threads=new Thread[tableIDs.length];

    /**
     * The last time each table was invalidated.
     */
    private static final long[] invalidatedTos=new long[tableIDs.length];
    
    /**
     * The last time the table was sychronized successfully.
     */
    private static final long[] updatedTos=new long[tableIDs.length];

    public static void start() {
        Profiler.startProfile(Profiler.UNKNOWN, BackupDatabaseSynchronizer.class, "start()", null);
        try {
            synchronized(System.out) {
                if(threads[0]==null) {
                    System.out.print("Starting BackupDatabaseSynchronizer: ");
                    for(int c=0;c<tableIDs.length;c++) {
                        Thread thread=threads[c]=new Thread(new BackupDatabaseSynchronizer(c));
                        thread.setPriority(Thread.NORM_PRIORITY-1);
                        thread.start();
                    }
                    System.out.println("Done");
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void invalidateTable(int tableID) {
        Profiler.startProfile(Profiler.FAST, BackupDatabaseSynchronizer.class, "invalidateTable(int)", null);
        try {
            for(int c=0;c<tableIDs.length;c++) {
                if(tableID==tableIDs[c]) {
                    Thread thread=threads[c];
                    synchronized(thread) {
                        invalidatedTos[c]=System.currentTimeMillis();
                        thread.notify();
                    }
                    break;
                }
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    private final int index;
    
    private BackupDatabaseSynchronizer(int index) {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupDatabaseSynchronizer.class, "<init>(int)", null);
        try {
            this.index=index;
        } finally {
            Profiler.endProfile(Profiler.INSTANTANEOUS);
        }
    }
    
    public void run() {
        Profiler.startProfile(Profiler.UNKNOWN, BackupDatabaseSynchronizer.class, "run()", null);
        try {
            while(true) {
                final Thread thisThread=threads[index];
                try {
                    Random random=MasterServer.getRandom();
                    while(true) {
                        try {
                            // Delay for some random time
                            thisThread.sleep(10000+random.nextInt(50000));
                        } catch(InterruptedException err) {
                            MasterServer.reportWarning(err, null);
                        }

                        long updatedTime=System.currentTimeMillis();

                        ProcessTimer timer=new ProcessTimer(
                            random,
                            MasterConfiguration.getWarningSmtpServer(),
                            MasterConfiguration.getWarningEmailFrom(),
                            MasterConfiguration.getWarningEmailTo(),
                            "Backup Database Synchronizer",
                            "Synchronizing table #"+tableIDs[index],
                            TIMER_MAX_TIME,
                            TIMER_REMINDER_INTERVAL
                        );
                        try {
                            timer.start();
                            MasterDatabaseConnection conn=(MasterDatabaseConnection)MasterDatabase.getDatabase().createDatabaseConnection();
                            try {
                                BackupDatabaseConnection backupConn=(BackupDatabaseConnection)BackupDatabase.getDatabase().createDatabaseConnection();
                                try {
                                    boolean connRolledBack=false;
                                    boolean backupConnRolledBack=false;
                                    try {
                                        synchronizeTable(conn, backupConn);
                                    } catch(IOException err) {
                                        if(conn.rollbackAndClose()) {
                                            connRolledBack=true;
                                        }
                                        if(backupConn.rollbackAndClose()) {
                                            backupConnRolledBack=true;
                                        }
                                        throw err;
                                    } catch(SQLException err) {
                                        if(conn.rollbackAndClose()) {
                                            connRolledBack=true;
                                        }
                                        if(backupConn.rollbackAndClose()) {
                                            backupConnRolledBack=true;
                                        }
                                        throw err;
                                    } finally {
                                        if(!connRolledBack && !conn.isClosed()) conn.commit();
                                        if(!backupConnRolledBack && !backupConn.isClosed()) backupConn.commit();
                                    }
                                } finally {
                                    backupConn.releaseConnection();
                                }
                            } finally {
                                conn.releaseConnection();
                            }
                        } finally {
                            timer.stop();
                        }

                        // Wait to be notified, maximum 12 hours before auto resync.
                        synchronized(thisThread) {
                            updatedTos[index]=updatedTime;
                            if(updatedTime>invalidatedTos[index]) {
                                thisThread.wait(12L*60*60*1000);
                            }
                        }
                    }
                } catch(ThreadDeath TD) {
                    throw TD;
                } catch(Throwable T) {
                    MasterServer.reportError(T, null);
                }
                try {
                    thisThread.sleep(60000);
                } catch(InterruptedException err) {
                    MasterServer.reportWarning(err, null);
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    private void synchronizeTable(MasterDatabaseConnection conn, BackupDatabaseConnection backupConn) throws IOException, SQLException {
        String tableName=TableHandler.getTableName(conn, tableIDs[index]);
        Profiler.startProfile(Profiler.UNKNOWN, BackupDatabaseSynchronizer.class, "synchronizeTable(MasterDatabaseConnection,BackupDatabaseConnection)", tableName);
        try {
            String primaryKeyColumn=primaryKeyColumns[index];
            String[] columns=BackupDatabaseSynchronizer.columns[index];
            Class[] columnTypes=BackupDatabaseSynchronizer.columnTypes[index];
            int pkeyColumnIndex=-1;
            for(int c=0;c<columns.length;c++) {
                if(columns[c].equals(primaryKeyColumn)) {
                    pkeyColumnIndex=c;
                    break;
                }
            }
            if(pkeyColumnIndex==-1) throw new SQLException("Unable to find primary key column in column list: "+primaryKeyColumn);

            // The sql query used to get all rows
            StringBuilder sql=new StringBuilder();
            sql.append("select ");
            for(int c=0;c<columns.length;c++) {
                if(c>0) sql.append(", ");
                sql.append(columns[c]);
            }
            sql.append(" from ").append(tableName).append(" order by ").append(primaryKeyColumn);
            String sqlString=sql.toString();

            // Get the data from the source table, sorted by pkey
            Map<Object,Object[]> masterRows=loadRows(conn, sqlString, pkeyColumnIndex, columns.length, columnTypes);
            
            // Get the data from the destination table, sorted by pkey
            Map<Object,Object[]> backupRows=loadRows(backupConn, sqlString, pkeyColumnIndex, columns.length, columnTypes);

            // Insert and update the backup mirror, then commit
            Iterator<Object> pkeys=masterRows.keySet().iterator();
            while(pkeys.hasNext()) {
                Object pkey=pkeys.next();
                Object[] row=masterRows.get(pkey);
                Object[] existingRow=backupRows.get(pkey);
                if(existingRow==null) {
                    // Add if the row doesn't exist
                    sql.setLength(0);
                    sql.append("insert into ").append(tableName).append(" values(");
                    for(int c=0;c<columns.length;c++) {
                        if(c>0) sql.append(',');
                        sql.append('?');
                    }
                    sql.append(')');
                    PreparedStatement pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement(sql.toString());
                    try {
                        for(int c=0;c<columns.length;c++) doPreparedSet(pstmt, c+1, row[c], columnTypes[c]);
                        backupConn.incrementUpdateCount();
                        pstmt.executeUpdate();
                    } catch(SQLException err) {
                        System.err.println("Error from insert: "+pstmt.toString());
                        throw err;
                    } finally {
                        pstmt.close();
                    }
                } else {
                    // Make sure all values match
                    boolean isMatch=true;
                    for(int c=0;c<columns.length;c++) {
                        if(!StringUtility.equals(row[c], existingRow[c])) {
                            isMatch=false;
                            break;
                        }
                    }
                    if(!isMatch) {
                        sql.setLength(0);
                        sql.append("update ").append(tableName).append(" set ");
                        boolean didOne=false;
                        for(int c=0;c<columns.length;c++) {
                            if(!StringUtility.equals(row[c], existingRow[c])) {
                                if(!didOne) didOne=true;
                                else sql.append(", ");
                                sql.append(columns[c]).append("=?");
                            }
                        }
                        sql.append(" where ").append(primaryKeyColumn).append("=?");
                        PreparedStatement pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement(sql.toString());
                        try {
                            int count=1;
                            for(int c=0;c<columns.length;c++) {
                                if(!StringUtility.equals(row[c], existingRow[c])) {
                                    doPreparedSet(pstmt, count++, row[c], columnTypes[c]);
                                }
                            }
                            doPreparedSet(pstmt, count, row[pkeyColumnIndex], columnTypes[pkeyColumnIndex]);
                            backupConn.incrementUpdateCount();
                            pstmt.executeUpdate();
                        } catch(SQLException err) {
                            System.err.println("Error from insert: "+pstmt.toString());
                            throw err;
                        } finally {
                            pstmt.close();
                        }
                    }
                }
                // Remove so the row will not be deleted
                backupRows.remove(pkey);
            }
            backupConn.commit();
            backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).setAutoCommit(true);
            backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).setAutoCommit(false);
            
            // Delete extras, then commit
            if(!backupRows.isEmpty()) {
                sql.setLength(0);
                sql.append("delete from ").append(tableName).append(" where ").append(primaryKeyColumn).append(" in (");
                Iterator<Object> extraPKeys=backupRows.keySet().iterator();
                boolean didOne=false;
                while(extraPKeys.hasNext()) {
                    Object extraPKey=extraPKeys.next();
                    if(didOne) sql.append(',');
                    else didOne=true;
                    sql.append('?');
                }
                sql.append(')');
                PreparedStatement pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement(sql.toString());
                try {
                    int count=1;
                    extraPKeys=backupRows.keySet().iterator();
                    while(extraPKeys.hasNext()) {
                        Object extraPKey=extraPKeys.next();
                        doPreparedSet(pstmt, count++, extraPKey, columnTypes[pkeyColumnIndex]);
                    }
                    backupConn.incrementUpdateCount();
                    pstmt.executeUpdate();
                } catch(SQLException err) {
                    System.err.println("Error from delete: "+pstmt.toString());
                    throw err;
                } finally {
                    pstmt.close();
                }
                backupConn.commit();
                backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).setAutoCommit(true);
                backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).setAutoCommit(false);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    /**
     * Loads all of the rows, returns in a Map<String,String[]> or pkey and then column data.
     */
    private static Map<Object,Object[]> loadRows(DatabaseConnection conn, String sql, int pkeyColumnIndex, int columnCount, Class[] columnTypes) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupDatabaseSynchronizer.class, "loadRows(DatabaseConnection,String,int,int,Class[])", null);
        try {
            Statement stmt=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement();
            try {
                ResultSet results=stmt.executeQuery(sql);
                try {
                    Map<Object,Object[]> rows=new HashMap<Object,Object[]>();
                    while(results.next()) {
                        Object[] row=new Object[columnCount];
                        for(int c=0;c<columnCount;c++) {
                            Class type=columnTypes[c];
                            Object value;
                            if(type==Boolean.class) {
                                boolean b=results.getBoolean(c+1);
                                value=results.wasNull() ? null : Boolean.valueOf(b);
                            } else if(type==Integer.class) {
                                int i=results.getInt(c+1);
                                value=results.wasNull() ? null : Integer.valueOf(i);
                            } else if(type==Short.class) {
                                short s=results.getShort(c+1);
                                value=results.wasNull() ? null : Short.valueOf(s);
                            } else if(type==String.class) {
                                value=results.getString(c+1);
                            } else if(type==Timestamp.class) {
                                value=results.getTimestamp(c+1);
                            } else {
                                throw new RuntimeException("Unexpected type: " + type.getName());
                            }
                            row[c]=value;
                        }
                        rows.put(row[pkeyColumnIndex], row);
                    }
                    return rows;
                } finally {
                    results.close();
                }
            } catch(SQLException err) {
                System.err.println("Error from query: "+sql);
                throw err;
            } finally {
                stmt.close();
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    private static void doPreparedSet(PreparedStatement pstmt, int parameterIndex, Object value, Class type) throws SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupDatabaseSynchronizer.class, "doPreparedSet(PreparedStatement,int,Object,Class)", null);
        try {
            if(type==Boolean.class) {
                if(value==null) pstmt.setNull(parameterIndex, Types.BOOLEAN);
                else pstmt.setBoolean(parameterIndex, ((Boolean)value).booleanValue());
            } else if(type==Integer.class) {
                if(value==null) pstmt.setNull(parameterIndex, Types.INTEGER);
                else pstmt.setInt(parameterIndex, ((Integer)value).intValue());
            } else if(type==Short.class) {
                if(value==null) pstmt.setNull(parameterIndex, Types.SMALLINT);
                else pstmt.setShort(parameterIndex, ((Short)value).shortValue());
            } else if(type==String.class) {
                pstmt.setString(parameterIndex, (String)value);
            } else if(type==Timestamp.class) {
                pstmt.setTimestamp(parameterIndex, (Timestamp)value);
            } else {
                throw new RuntimeException("Unexpected type: " + type.getName());
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}
