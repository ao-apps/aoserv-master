package com.aoindustries.aoserv.master;

/*
 * Copyright 2004-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.cron.CronDaemon;
import com.aoindustries.cron.CronJob;
import com.aoindustries.email.*;
import com.aoindustries.profiler.*;
import com.aoindustries.util.*;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * Automatically cleans out old backup data.
 *
 * @author  AO Industries, Inc.
 */
public final class BackupCleaner implements CronJob {

    /**
     * Runs at 6:25 am daily.
     */
    public boolean isCronJobScheduled(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
        return
            minute==25
            && hour==6
        ;
    }

    public int getCronJobScheduleMode() {
        return CRON_JOB_SCHEDULE_SKIP;
    }

    public String getCronJobName() {
        return "BackupCleaner";
    }

    public int getCronJobThreadPriority() {
        return Thread.NORM_PRIORITY-1;
    }

    /**
     * The maximum time for a cleaning.
     */
    private static final long TIMER_MAX_TIME=60L*60*1000;

    /**
     * The interval in which the administrators will be reminded.
     */
    private static final long TIMER_REMINDER_INTERVAL=6L*60*60*1000;

    private static boolean started=false;

    public static void start() {
        Profiler.startProfile(Profiler.UNKNOWN, BackupCleaner.class, "start()", null);
        try {
            synchronized(System.out) {
                if(!started) {
                    System.out.print("Starting BackupCleaner: ");
                    CronDaemon.addCronJob(new BackupCleaner(), MasterServer.getErrorHandler());
                    started=true;
                    System.out.println("Done");
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    private BackupCleaner() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, BackupCleaner.class, "<init>()", null);
        Profiler.endProfile(Profiler.INSTANTANEOUS);
    }
    
    public void runCronJob(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
        Profiler.startProfile(Profiler.UNKNOWN, BackupCleaner.class, "runCronJob(int,int,int,int,int,int)", null);
        try {
            try {
                ProcessTimer timer=new ProcessTimer(
                    MasterServer.getRandom(),
                    MasterConfiguration.getWarningSmtpServer(),
                    MasterConfiguration.getWarningEmailFrom(),
                    MasterConfiguration.getWarningEmailTo(),
                    "Backup Cleaner",
                    "Cleaning old backup resources",
                    TIMER_MAX_TIME,
                    TIMER_REMINDER_INTERVAL
                );
                try {
                    timer.start();

                    // Start the transaction
                    InvalidateList invalidateList=new InvalidateList();
                    MasterDatabaseConnection conn=(MasterDatabaseConnection)MasterDatabase.getDatabase().createDatabaseConnection();
                    try {
                        BackupDatabaseConnection backupConn=(BackupDatabaseConnection)BackupDatabase.getDatabase().createDatabaseConnection();
                        try {
                            boolean connRolledBack=false;
                            boolean backupConnRolledBack=false;
                            try {
                                BackupHandler.removeUnusedBackupData(conn, backupConn, invalidateList);
                            } catch(IOException err) {
                                if(conn.rollbackAndClose()) {
                                    connRolledBack=true;
                                    invalidateList=null;
                                }
                                if(backupConn.rollbackAndClose()) {
                                    backupConnRolledBack=true;
                                    invalidateList=null;
                                }
                                throw err;
                            } catch(SQLException err) {
                                if(conn.rollbackAndClose()) {
                                    connRolledBack=true;
                                    invalidateList=null;
                                }
                                if(backupConn.rollbackAndClose()) {
                                    backupConnRolledBack=true;
                                    invalidateList=null;
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
                    if(invalidateList!=null) MasterServer.invalidateTables(invalidateList, null);
                } finally {
                    timer.stop();
                }
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