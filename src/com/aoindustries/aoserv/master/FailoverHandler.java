package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2006 by AO Industries, Inc.,
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
            String mustring = source.getUsername();
            MasterUser mu = MasterServer.getMasterUser(conn, mustring);
            if (mu==null) throw new SQLException("User "+mustring+" is not master user and may not access failover_file_log.");
            int aoServer=getFromAOServerForFailoverFileReplication(conn, replication);
            ServerHandler.checkAccessServer(conn, source, "add_failover_file_log", aoServer);

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
                SchemaTable.FAILOVER_FILE_LOG,
                ServerHandler.getBusinessesForServer(conn, aoServer),
                aoServer,
                false
            );
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    public static int getFromAOServerForFailoverFileReplication(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, FailoverHandler.class, "getFromAOServerForFailoverFileReplication(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select from_server from failover_file_replications where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getToAOServerForFailoverFileReplication(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, FailoverHandler.class, "getToAOServerForFailoverFileReplication(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select to_server from failover_file_replications where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setLastFailoverReplicationTime(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ffr,
        long time
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, FailoverHandler.class, "setLastFailoverReplicationTime(MasterDatabaseConnection,RequestSource,InvalidateList,int,long)", null);
        try {
            String username=source.getUsername();
            MasterUser masterUser=MasterServer.getMasterUser(conn, username);
            if(masterUser==null) throw new SQLException("Only master users allowed to set the last failover replication time.");
            int aoServer=getFromAOServerForFailoverFileReplication(conn, ffr);
            ServerHandler.checkAccessServer(conn, source, "setLastBackupTime", aoServer);

            conn.executeUpdate(
                "update failover_file_replications set last_start_time=? where pkey=?",
                new Timestamp(time),
                ffr
            );
            invalidateList.addTable(
                conn,
                SchemaTable.FAILOVER_FILE_REPLICATIONS,
                ServerHandler.getBusinessesForServer(conn, aoServer),
                aoServer,
                false
            );
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
            int fromServer = getFromAOServerForFailoverFileReplication(conn, replication);
            ServerHandler.checkAccessServer(conn, source, "getFailoverFileLogs", fromServer);

            MasterServer.writeObjects(conn, source, out, false, new FailoverFileLog(), "select * from failover_file_log where replication=? order by start_time desc limit ?", replication, maxRows);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Runs at 1:20 am daily.
     */
    public boolean isCronJobScheduled(int minute, int hour, int dayOfMonth, int month, int dayOfWeek) {
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

    public void runCronJob(int minute, int hour, int dayOfMonth, int month, int dayOfWeek) {
        Profiler.startProfile(Profiler.UNKNOWN, FailoverHandler.class, "runCronJob(int,int,int,int,int)", null);
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
