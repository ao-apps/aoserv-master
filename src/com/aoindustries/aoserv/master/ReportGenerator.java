package com.aoindustries.aoserv.master;

/*
 * Copyright 2003-2007 by AO Industries, Inc.,
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
 * Automatically generates various reports on a regular basis.  These reports are then
 * used by the accounting system to charge the appropriate amount.  These reports may be missed
 * and will not be created when missed.  Anything depending on these reports should get
 * its information from the reports that are available without depending on all reports
 * being present.  It is an acceptable error condition if not a single report in a month
 * has occurred.
 *
 * @author  AO Industries, Inc.
 */
final public class ReportGenerator implements CronJob {

    /**
     * The maximum time for a backup reporting.
     */
    private static final long BACKUP_REPORT_MAX_TIME=2L*60*60*1000;

    /**
     * The interval in which the administrators will be reminded.
     */
    private static final long TIMER_REMINDER_INTERVAL=12L*60*60*1000;

    private static boolean started=false;

    public static void start() {
        Profiler.startProfile(Profiler.UNKNOWN, ReportGenerator.class, "start()", null);
        try {
            synchronized(System.out) {
                if(!started) {
                    System.out.print("Starting ReportGenerator: ");
                    CronDaemon.addCronJob(new ReportGenerator(), MasterServer.getErrorHandler());
                    started=true;
                    System.out.println("Done");
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    private ReportGenerator() {
        Profiler.startProfile(Profiler.INSTANTANEOUS, ReportGenerator.class, "<init>()", null);
        Profiler.endProfile(Profiler.INSTANTANEOUS);
    }
    
    /**
     * Runs at <code>BackupReport.BACKUP_REPORT_HOUR</code>:<code>BackupReport.BACKUP_REPORT_MINUTE</code> am daily.
     */
    public boolean isCronJobScheduled(int minute, int hour, int dayOfMonth, int month, int dayOfWeek) {
        return
            minute==BackupReport.BACKUP_REPORT_MINUTE
            && hour==BackupReport.BACKUP_REPORT_HOUR
        ;
    }

    public int getCronJobScheduleMode() {
        return CRON_JOB_SCHEDULE_SKIP;
    }

    public String getCronJobName() {
        return "ReportGenerator";
    }

    public int getCronJobThreadPriority() {
        return Thread.NORM_PRIORITY-2;
    }

    public void runCronJob(int minute, int hour, int dayOfMonth, int month, int dayOfWeek) {
        Profiler.startProfile(Profiler.UNKNOWN, ReportGenerator.class, "runCronJob(int,int,int,int,int)", null);
        try {
            try {
                ProcessTimer timer=new ProcessTimer(
                    MasterServer.getRandom(),
                    MasterConfiguration.getWarningSmtpServer(),
                    MasterConfiguration.getWarningEmailFrom(),
                    MasterConfiguration.getWarningEmailTo(),
                    "Backup Report Generator",
                    "Generating contents for backup_reports",
                    BACKUP_REPORT_MAX_TIME,
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
                                long currentTimeMillis=System.currentTimeMillis();
                                Timestamp now=new Timestamp(currentTimeMillis);
                                
                                // Do not make the run twice in one day
                                if(
                                    backupConn.executeBooleanQuery(
                                        "select\n"
                                        + "  (\n"
                                        + "    select\n"
                                        + "      pkey\n"
                                        + "    from\n"
                                        + "      backup_reports\n"
                                        + "    where\n"
                                        + "      ?::date=date\n"
                                        + "    limit 1\n"
                                        + "  ) is null",
                                        now
                                    )
                                ) {
                                    // HashMap keyed on server, containing HashMaps keyed on package, containing TempBackupReport objects
                                    Map<Integer,Map<Integer,TempBackupReport>> stats=new HashMap<Integer,Map<Integer,TempBackupReport>>();

                                    String sqlString=null;
                                    Statement stmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement();
                                    try {
                                        // First, count up the total number of files per server and per package
                                        backupConn.incrementQueryCount();
                                        ResultSet results=stmt.executeQuery(sqlString="select server, package, count(*) from file_backups group by server, package");
                                        try {
                                            while(results.next()) {
                                                int server=results.getInt(1);
                                                int packageNum=results.getInt(2);
                                                int fileCount=results.getInt(3);

                                                TempBackupReport tbr=new TempBackupReport();
                                                tbr.server=server;
                                                tbr.packageNum=packageNum;
                                                tbr.fileCount=fileCount;

                                                Integer serverInteger=Integer.valueOf(server);
                                                Map<Integer,TempBackupReport> packages=stats.get(serverInteger);
                                                if(packages==null) stats.put(serverInteger, packages=new HashMap<Integer,TempBackupReport>());
                                                packages.put(Integer.valueOf(packageNum), tbr);
                                            }
                                        } finally {
                                            results.close();
                                        }

                                        // Count up the data sizes by server and package
                                        backupConn.incrementQueryCount();
                                        results=stmt.executeQuery(
                                            sqlString=
                                              "select\n"
                                            + "  fb.server,\n"
                                            + "  fb.package,\n"
                                            + "  sum(bd.data_size),\n"
                                            + "  sum(coalesce(bd.compressed_size, bd.data_size)),\n"
                                            + "  sum(\n"
                                            + "    case when\n"
                                            + "      (coalesce(bd.compressed_size, bd.data_size)%(4096::int8))=0\n"
                                            + "    then\n"
                                            + "      coalesce(bd.compressed_size, bd.data_size)\n"
                                            + "    else\n"
                                            + "      ((coalesce(bd.compressed_size, bd.data_size)/4096)+1)*4096\n"
                                            + "    end\n"
                                            + "  )\n"
                                            + "from\n"
                                            + "  (\n"
                                            + "    select\n"
                                            + "      server,\n"
                                            + "      package,\n"
                                            + "      backup_data\n"
                                            + "    from\n"
                                            + "      file_backups\n"
                                            + "    where\n"
                                            + "      backup_data is not null\n"
                                            + "    union select\n"
                                            + "      ao_server,\n"
                                            + "      package,\n"
                                            + "      backup_data\n"
                                            + "    from\n"
                                            + "      interbase_backups\n"
                                            + "    union select\n"
                                            + "      ms.ao_server,\n"
                                            + "      mb.package,\n"
                                            + "      mb.backup_data\n"
                                            + "    from\n"
                                            + "      mysql_backups mb,\n"
                                            + "      mysql_servers ms\n"
                                            + "    where\n"
                                            + "      mb.mysql_server=ms.pkey\n"
                                            + "    union select\n"
                                            + "      ps.ao_server,\n"
                                            + "      pb.package,\n"
                                            + "      pb.backup_data\n"
                                            + "    from\n"
                                            + "      postgres_backups pb,\n"
                                            + "      postgres_servers ps\n"
                                            + "    where\n"
                                            + "      pb.postgres_server=ps.pkey\n"
                                            + "  ) as fb,\n"
                                            + "  backup_data bd\n"
                                            + "where\n"
                                            + "  fb.backup_data=bd.pkey\n"
                                            + "  and bd.is_stored\n"
                                            + "group by\n"
                                            + "  fb.server,\n"
                                            + "  fb.package"
                                        );
                                        try {
                                            while(results.next()) {
                                                int server=results.getInt(1);
                                                int packageNum=results.getInt(2);
                                                long uncompressedSize=results.getLong(3);
                                                long compressedSize=results.getLong(4);
                                                long diskSize=results.getLong(5);

                                                Integer serverInteger=Integer.valueOf(server);
                                                Map<Integer,TempBackupReport> packages=stats.get(serverInteger);
                                                if(packages==null) stats.put(serverInteger, packages=new HashMap<Integer,TempBackupReport>());
                                                Integer packageInteger=Integer.valueOf(packageNum);
                                                TempBackupReport tbr=(TempBackupReport)packages.get(packageInteger);
                                                if(tbr==null) {
                                                    tbr=new TempBackupReport();
                                                    tbr.server=server;
                                                    tbr.packageNum=packageNum;
                                                    packages.put(packageInteger, tbr);
                                                }
                                                tbr.uncompressedSize=uncompressedSize;
                                                tbr.compressedSize=compressedSize;
                                                tbr.diskSize=diskSize;
                                            }
                                        } finally {
                                            results.close();
                                        }
                                    } catch(SQLException err) {
                                        System.err.println("Error from query: "+sqlString);
                                        throw err;
                                    } finally {
                                        stmt.close();
                                    }

                                    // Add these stats to the table
                                    PreparedStatement pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into backup_reports values(nextval('backup_reports_pkey_seq'),?,?,?::date,?,?::int8,?::int8,?::int8);");
                                    try {
                                        Iterator<Integer> serverKeys=stats.keySet().iterator();
                                        while(serverKeys.hasNext()) {
                                            Map<Integer,TempBackupReport> packages=stats.get(serverKeys.next());
                                            Iterator<Integer> packageKeys=packages.keySet().iterator();
                                            while(packageKeys.hasNext()) {
                                                TempBackupReport tbr=packages.get(packageKeys.next());
                                                pstmt.setInt(1, tbr.server);
                                                pstmt.setInt(2, tbr.packageNum);
                                                pstmt.setTimestamp(3, now);
                                                pstmt.setInt(4, tbr.fileCount);
                                                pstmt.setLong(5, tbr.uncompressedSize);
                                                pstmt.setLong(6, tbr.compressedSize);
                                                pstmt.setLong(7, tbr.diskSize);
                                                pstmt.addBatch();
                                            }
                                        }
                                        backupConn.incrementUpdateCount();
                                        pstmt.executeBatch();
                                    } catch(SQLException err) {
                                        System.err.println("Error from update: "+pstmt.toString());
                                        throw err;
                                    } finally {
                                        pstmt.close();
                                    }

                                    // Invalidate the table
                                    invalidateList.addTable(conn, SchemaTable.BACKUP_REPORTS, InvalidateList.allBusinesses, InvalidateList.allServers, false);
                                }
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