package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.FailoverFileLog;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.cron.CronDaemon;
import com.aoindustries.cron.CronJob;
import com.aoindustries.io.BitRateProvider;
import com.aoindustries.io.CompressedDataOutputStream;
import com.aoindustries.util.IntList;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

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
        //String mustring = source.getUsername();
        //MasterUser mu = MasterServer.getMasterUser(conn, mustring);
        //if (mu==null) throw new SQLException("User "+mustring+" is not master user and may not access failover_file_log.");

        // The server must be an exact package match to allow adding log entries
        int server=getFromServerForFailoverFileReplication(conn, replication);
        String userPackage = UsernameHandler.getPackageForUsername(conn, source.getUsername());
        String serverPackage = PackageHandler.getNameForPackage(conn, ServerHandler.getPackageForServer(conn, server));
        if(!userPackage.equals(serverPackage)) throw new SQLException("userPackage!=serverPackage: may only set failover_file_log for servers that have the same package as the business_administrator adding the log entry");
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
    }
    
    public static void setFailoverFileReplicationBitRate(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        int bitRate
    ) throws IOException, SQLException {
        if(
            bitRate!=BitRateProvider.UNLIMITED_BANDWIDTH
            && bitRate<BitRateProvider.MINIMUM_BIT_RATE
        ) throw new SQLException("Bit rate too low: "+bitRate+"<"+BitRateProvider.MINIMUM_BIT_RATE);

        // The server must be an exact package match to allow setting the bit rate
        int server=getFromServerForFailoverFileReplication(conn, pkey);
        String userPackage = UsernameHandler.getPackageForUsername(conn, source.getUsername());
        String serverPackage = PackageHandler.getNameForPackage(conn, ServerHandler.getPackageForServer(conn, server));
        if(!userPackage.equals(serverPackage)) throw new SQLException("userPackage!=serverPackage: may only set failover_file_replications.max_bit_rate for servers that have the same package as the business_administrator setting the bit rate");

        if(bitRate==BitRateProvider.UNLIMITED_BANDWIDTH) conn.executeUpdate("update failover_file_replications set max_bit_rate=null where pkey=?", pkey);
        else conn.executeUpdate("update failover_file_replications set max_bit_rate=? where pkey=?", bitRate, pkey);

        // Notify all clients of the update
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.FAILOVER_FILE_REPLICATIONS,
            ServerHandler.getBusinessesForServer(conn, server),
            server,
            false
        );
    }

    public static void setFailoverFileSchedules(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int replication,
        List<Short> hours,
        List<Short> minutes
    ) throws IOException, SQLException {
        // The server must be an exact package match to allow setting the schedule
        int server=getFromServerForFailoverFileReplication(conn, replication);
        String userPackage = UsernameHandler.getPackageForUsername(conn, source.getUsername());
        String serverPackage = PackageHandler.getNameForPackage(conn, ServerHandler.getPackageForServer(conn, server));
        if(!userPackage.equals(serverPackage)) throw new SQLException("userPackage!=serverPackage: may only set failover_file_schedule for servers that have the same package as the business_administrator setting the schedule");

        // If not modified, invalidation will not be performed
        boolean modified = false;

        // Get the list of all the pkeys that currently exist
        IntList pkeys = conn.executeIntListQuery("select pkey from failover_file_schedule where replication=?", replication);
        int size = hours.size();
        for(int c=0;c<size;c++) {
            // If it exists, remove pkey from the list, otherwise add
            short hour = hours.get(c);
            short minute = minutes.get(c);
            int existingPkey = conn.executeIntQuery(
                "select coalesce((select pkey from failover_file_schedule where replication=? and hour=? and minute=?), -1)",
                replication,
                hour,
                minute
            );
            if(existingPkey==-1) {
                // Doesn't exist, add
                conn.executeUpdate("insert into failover_file_schedule (replication, hour, minute, enabled) values(?,?,?,true)", replication, hour, minute);
                modified = true;
            } else {
                // Remove from the list that will be removed
                if(!pkeys.removeByValue(existingPkey)) throw new SQLException("pkeys doesn't contain pkey="+existingPkey);
            }
        }
        // Delete the unmatched pkeys
        if(pkeys.size()>0) {
            for(int c=0,len=pkeys.size(); c<len; c++) {
                conn.executeUpdate("delete from failover_file_schedule where pkey=?", pkeys.getInt(c));
            }
            modified = true;
        }

        // Notify all clients of the update
        if(modified) {
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.FAILOVER_FILE_SCHEDULE,
                ServerHandler.getBusinessesForServer(conn, server),
                server,
                false
            );
        }
    }

    public static void setFileBackupSettingsAllAtOnce(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int replication,
        List<String> paths,
        List<Boolean> backupEnableds
    ) throws IOException, SQLException {
        // The server must be an exact package match to allow setting the schedule
        int server=getFromServerForFailoverFileReplication(conn, replication);
        String userPackage = UsernameHandler.getPackageForUsername(conn, source.getUsername());
        String serverPackage = PackageHandler.getNameForPackage(conn, ServerHandler.getPackageForServer(conn, server));
        if(!userPackage.equals(serverPackage)) throw new SQLException("userPackage!=serverPackage: may only set file_backup_settings for servers that have the same package as the business_administrator making the settings");

        // If not modified, invalidation will not be performed
        boolean modified = false;

        // Get the list of all the pkeys that currently exist
        IntList pkeys = conn.executeIntListQuery("select pkey from file_backup_settings where replication=?", replication);
        int size = paths.size();
        for(int c=0;c<size;c++) {
            // If it exists, remove pkey from the list, otherwise add
            String path = paths.get(c);
            boolean backupEnabled = backupEnableds.get(c);
            int existingPkey = conn.executeIntQuery(
                "select coalesce((select pkey from file_backup_settings where replication=? and path=?), -1)",
                replication,
                path
            );
            if(existingPkey==-1) {
                // Doesn't exist, add
                conn.executeUpdate("insert into file_backup_settings (replication, path, backup_enabled) values(?,?,?)", replication, path, backupEnabled);
                modified = true;
            } else {
                // Update the enabled flag if it doesn't match
                if(
                    conn.executeUpdate(
                        "update file_backup_settings set backup_enabled=? where pkey=? and not backup_enabled=?",
                        backupEnabled,
                        existingPkey,
                        backupEnabled
                    )==1
                ) modified = true;

                // Remove from the list that will be removed
                if(!pkeys.removeByValue(existingPkey)) throw new SQLException("pkeys doesn't contain pkey="+existingPkey);
            }
        }
        // Delete the unmatched pkeys
        if(pkeys.size()>0) {
            for(int c=0,len=pkeys.size(); c<len; c++) {
                conn.executeUpdate("delete from file_backup_settings where pkey=?", pkeys.getInt(c));
            }
            modified = true;
        }

        // Notify all clients of the update
        if(modified) {
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.FILE_BACKUP_SETTINGS,
                ServerHandler.getBusinessesForServer(conn, server),
                server,
                false
            );
        }
    }

    public static int getFromServerForFailoverFileReplication(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeIntQuery("select server from failover_file_replications where pkey=?", pkey);
    }

    public static int getBackupPartitionForFailoverFileReplication(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeIntQuery("select backup_partition from failover_file_replications where pkey=?", pkey);
    }

    public static void getFailoverFileLogs(
        MasterDatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        int replication,
        int maxRows
    ) throws IOException, SQLException {
        // Check access for the from server
        int fromServer = getFromServerForFailoverFileReplication(conn, replication);
        ServerHandler.checkAccessServer(conn, source, "getFailoverFileLogs", fromServer);

        MasterServer.writeObjects(conn, source, out, false, new FailoverFileLog(), "select * from failover_file_log where replication=? order by start_time desc limit ?", replication, maxRows);
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
        synchronized(System.out) {
            if(!started) {
                System.out.print("Starting FailoverHandler: ");
                CronDaemon.addCronJob(new FailoverHandler(), MasterServer.getErrorHandler());
                started=true;
                System.out.println("Done");
            }
        }
    }

    private FailoverHandler() {
    }

    public void runCronJob(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
        try {
            MasterDatabase.getDatabase().executeUpdate("delete from failover_file_log where end_time <= (now()-'1 year'::interval)");
        } catch(ThreadDeath TD) {
            throw TD;
        } catch(Throwable T) {
            MasterServer.reportError(T, null);
        }
    }
}
