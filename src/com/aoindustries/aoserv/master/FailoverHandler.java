package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.cron.CronDaemon;
import com.aoindustries.cron.CronJob;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.profiler.*;
import java.io.*;
import java.sql.*;

/**
 * The <code>FailoverHandler</code> handles all the accesses to the failover tables.
 *
 * @author  AO Industries, Inc.
 */
final public class FailoverHandler implements CronJob {

    public static int addFailoverFileLog(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int replication,
        long startTime,
        long endTime,
        int scanned,
        int updated,
        long bytes,
        boolean isSuccessful
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, FailoverHandler.class, "addFailoverFileLog(MasterDatabaseConnection,RequestSource,InvalidateList,int,long,long,int,int,long,boolean)", null);
        try {
            //String mustring = source.getUsername();
            //MasterUser mu = MasterServer.getMasterUser(conn, mustring);
            //if (mu==null) throw new SQLException("User "+mustring+" is not master user and may not access failover_file_log.");
            
            // The server must be an exact package match to allow adding log entries
            int server=getFromServerForFailoverFileReplication(conn, replication);
            String userPackage = UsernameHandler.getPackageForUsername(conn, source.getUsername());
            String serverPackage = PackageHandler.getNameForPackage(conn, ServerHandler.getPackageForServer(conn, server));
            if(!userPackage.equals(serverPackage)) throw new SQLException("userPackage!=serverPackage: may only set failover_file_log for servers that have the same package and the business_administrator adding the log entry");
            //ServerHandler.checkAccessServer(conn, source, "add_failover_file_log", server);

            int pkey = conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('failover_file_log_pkey_seq')");
            conn.executeUpdate(
                "insert into\n"
                + "  failover_file_log\n"
                + "values(\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?,\n"
                + "  ?\n"
                + ")",
                pkey,
                replication,
                new Timestamp(startTime),
                new Timestamp(endTime),
                scanned,
                updated,
                bytes,
                isSuccessful
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.FAILOVER_FILE_LOG,
                ServerHandler.getBusinessesForServer(conn, server),
                server,
                false
            );
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    public static int getFromServerForFailoverFileReplication(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, FailoverHandler.class, "getFromServerForFailoverFileReplication(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select server from failover_file_replications where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getBackupPartitionForFailoverFileReplication(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, FailoverHandler.class, "getBackupPartitionForFailoverFileReplication(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select backup_partition from failover_file_replications where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void getFailoverFileLogs(
        MasterDatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        int replication,
        int maxRows
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, FailoverHandler.class, "getFailoverFileLogs(MasterDatabaseConnection,RequestSource,CompressedDataOutputStream,int,int)", null);
        try {
            // Check access for the from server
            int fromServer = getFromServerForFailoverFileReplication(conn, replication);
            ServerHandler.checkAccessServer(conn, source, "getFailoverFileLogs", fromServer);

            MasterServer.writeObjects(conn, source, out, false, new FailoverFileLog(), "select * from failover_file_log where replication=? order by start_time desc limit ?", replication, maxRows);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Runs at 1:20 am daily.
     */
    public boolean isCronJobScheduled(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
        return
            minute==45
            && hour==1
        ;
    }

    public int getCronJobScheduleMode() {
        return CRON_JOB_SCHEDULE_SKIP;
    }

    public String getCronJobName() {
        return "FailoverHandler";
    }

    public int getCronJobThreadPriority() {
        return Thread.NORM_PRIORITY-1;
    }

    private static boolean started=false;
    
    public static void start() {
        Profiler.startProfile(Profiler.UNKNOWN, FailoverHandler.class, "start()", null);
        try {
            synchronized(System.out) {
                if(!started) {
                    System.out.print("Starting FailoverHandler: ");
                    CronDaemon.addCronJob(new FailoverHandler(), MasterServer.getErrorHandler());
                    started=true;
                    System.out.println("Done");
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    private FailoverHandler() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, FailoverHandler.class, "<init>()", null);
        Profiler.endProfile(Profiler.INSTANTANEOUS);
    }

    public void runCronJob(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
        Profiler.startProfile(Profiler.UNKNOWN, FailoverHandler.class, "runCronJob(int,int,int,int,int,int)", null);
        try {
            try {
                MasterDatabase.getDatabase().executeUpdate("delete from failover_file_log where end_time <= (now()-'1 year'::interval)");
            } catch(ThreadDeath TD) {
                throw TD;
            } catch(Throwable T) {
                MasterServer.reportError(T, null);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}
