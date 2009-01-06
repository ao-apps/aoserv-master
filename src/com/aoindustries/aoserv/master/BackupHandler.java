package com.aoindustries.aoserv.master;

/*
 * Copyright 2002-2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.profiler.Profiler;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * The <code>BackupHandler</code> manages the backup data.
 *
 * @author  AO Industries, Inc.
 */
public final class BackupHandler {

    public static int addFileBackupSetting(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int replication,
        String path,
        boolean backupEnabled
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "addFileBackupSetting(MasterDatabaseConnection,RequestSource,InvalidateList,int,String,boolean)", null);
        try {
            int server = conn.executeIntQuery("select server from failover_file_replications where pkey=?", replication);
            int packageNum = ServerHandler.getPackageForServer(conn, server);
            PackageHandler.checkAccessPackage(conn, source, "addFileBackupSetting", packageNum);

            path=path.trim();
            if(path.length()==0) throw new SQLException("Path may not be empty: "+path);
            int slashPos=path.indexOf('/');
            if(slashPos==-1) throw new SQLException("Path must contain a slash (/): "+path);
            // TODO: Check for windows roots: if(FilePathHandler.getRootNode(backupConn, path.substring(0, slashPos+1))==-1) throw new SQLException("Path does not start with a valid root: "+path);

            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('file_backup_settings_pkey_seq')");
            conn.executeUpdate(
                "insert into file_backup_settings values(?,?,?,?)",
                pkey,
                replication,
                path,
                backupEnabled
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.FILE_BACKUP_SETTINGS,
                PackageHandler.getBusinessForPackage(conn, packageNum),
                server,
                false
            );
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeFileBackupSetting(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "removeFileBackupSetting(MasterDatabaseConnection,RequestSource,InvalidateList,int)", null);
        try {
            int server = conn.executeIntQuery("select ffr.server from file_backup_settings fbs inner join failover_file_replications ffr on fbs.replication=ffr.pkey where fbs.pkey=?", pkey);
            int packageNum=ServerHandler.getPackageForServer(conn, server);
            PackageHandler.checkAccessPackage(conn, source, "removeFileBackupSetting", packageNum);

            removeFileBackupSetting(conn, invalidateList, pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void removeFileBackupSetting(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "removeFileBackupSetting(MasterDatabaseConnection,InvalidateList,int)", null);
        try {
            int server = conn.executeIntQuery("select ffr.server from file_backup_settings fbs inner join failover_file_replications ffr on fbs.replication=ffr.pkey where fbs.pkey=?", pkey);
            int packageNum=ServerHandler.getPackageForServer(conn, server);

            conn.executeUpdate("delete from file_backup_settings where pkey=?", pkey);

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.FILE_BACKUP_SETTINGS,
                PackageHandler.getBusinessForPackage(conn, packageNum),
                server,
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setFileBackupSettings(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        String path,
        boolean backupEnabled
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "addFileBackupSetting(MasterDatabaseConnection,RequestSource,InvalidateList,int,String,boolean)", null);
        try {
            int server = conn.executeIntQuery("select ffr.server from file_backup_settings fbs inner join failover_file_replications ffr on fbs.replication=ffr.pkey where fbs.pkey=?", pkey);
            int packageNum = ServerHandler.getPackageForServer(conn, server);
            PackageHandler.checkAccessPackage(conn, source, "setFileBackupSetting", packageNum);

            path=path.trim();
            if(path.length()==0) throw new SQLException("Path may not be empty: "+path);
            int slashPos=path.indexOf('/');
            if(slashPos==-1) throw new SQLException("Path must contain a slash (/): "+path);
            // TODO: Check for windows roots: if(FilePathHandler.getRootNode(backupConn, path.substring(0, slashPos+1))==-1) throw new SQLException("Path does not start with a valid root: "+path);

            conn.executeUpdate(
                "update\n"
                + "  file_backup_settings\n"
                + "set\n"
                + "  path=?,\n"
                + "  backup_enabled=?\n"
                + "where\n"
                + "  pkey=?",
                path,
                backupEnabled,
                pkey
            );

            // Notify all clients of the update
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.FILE_BACKUP_SETTINGS,
                PackageHandler.getBusinessForPackage(conn, packageNum),
                server,
                false
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static int getAOServerForBackupPartition(
        MasterDatabaseConnection conn,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, BackupHandler.class, "getAOServerForBackupPartition(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeIntQuery("select ao_server from backup_partitions where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getPathForBackupPartition(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "getPathForBackupPartition(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select path from backup_partitions where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static long getBackupPartitionTotalSize(
        MasterDatabaseConnection conn,
        RequestSource source,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "getBackupPartitionTotalSize(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            int aoServer=getAOServerForBackupPartition(conn, pkey);
            ServerHandler.checkAccessServer(conn, source, "getBackupPartitionTotalSize", aoServer);
            if(DaemonHandler.isDaemonAvailable(aoServer)) {
                String path=getPathForBackupPartition(conn, pkey);
                try {
                    return DaemonHandler.getDaemonConnector(conn, aoServer).getDiskDeviceTotalSize(path);
                } catch(IOException err) {
                    DaemonHandler.flagDaemonAsDown(aoServer);
                    MasterServer.reportError(
                        err,
                        new Object[] {
                            "pkey="+pkey,
                            "path="+path,
                            "aoServer="+aoServer
                        }
                    );
                    return -1;
                } catch(SQLException err) {
                    DaemonHandler.flagDaemonAsDown(aoServer);
                    MasterServer.reportError(
                        err,
                        new Object[] {
                            "pkey="+pkey,
                            "path="+path,
                            "aoServer="+aoServer
                        }
                    );
                    return -1;
                }
            } else return -1;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static long getBackupPartitionUsedSize(
        MasterDatabaseConnection conn,
        RequestSource source,
        int pkey
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, BackupHandler.class, "getBackupPartitionUsedSize(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            int aoServer=getAOServerForBackupPartition(conn, pkey);
            ServerHandler.checkAccessServer(conn, source, "getBackupPartitionUsedSize", aoServer);
            if(DaemonHandler.isDaemonAvailable(aoServer)) {
                String path=getPathForBackupPartition(conn, pkey);
                try {
                    return DaemonHandler.getDaemonConnector(conn, aoServer).getDiskDeviceUsedSize(path);
                } catch(IOException err) {
                    DaemonHandler.flagDaemonAsDown(aoServer);
                    MasterServer.reportError(
                        err,
                        new Object[] {
                            "pkey="+pkey,
                            "path="+path,
                            "aoServer="+aoServer
                        }
                    );
                    return -1;
                } catch(SQLException err) {
                    DaemonHandler.flagDaemonAsDown(aoServer);
                    MasterServer.reportError(
                        err,
                        new Object[] {
                            "pkey="+pkey,
                            "path="+path,
                            "aoServer="+aoServer
                        }
                    );
                    return -1;
                }
            } else return -1;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}
