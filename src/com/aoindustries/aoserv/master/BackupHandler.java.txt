package com.aoindustries.aoserv.master;

/*
 * Copyright 2002-2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.sql.DatabaseConnection;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The <code>BackupHandler</code> manages the backup data.
 *
 * @author  AO Industries, Inc.
 */
public final class BackupHandler {

    private static final Logger logger = LogFactory.getLogger(BackupHandler.class);

    private BackupHandler() {
    }

    public static int addFileBackupSetting(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int replication,
        String path,
        boolean backupEnabled
    ) throws IOException, SQLException {
        int server = conn.executeIntQuery("select server from failover_file_replications where pkey=?", replication);
        String accounting = ServerHandler.getBusinessForServer(conn, server);
        BusinessHandler.checkAccessBusiness(conn, source, "addFileBackupSetting", accounting);

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
            accounting,
            server,
            false
        );
        return pkey;
    }

    public static void removeFileBackupSetting(
        DatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        int server = conn.executeIntQuery("select ffr.server from file_backup_settings fbs inner join failover_file_replications ffr on fbs.replication=ffr.pkey where fbs.pkey=?", pkey);
        String accounting=ServerHandler.getBusinessForServer(conn, server);
        BusinessHandler.checkAccessBusiness(conn, source, "removeFileBackupSetting", accounting);

        removeFileBackupSetting(conn, invalidateList, pkey);
    }

    public static void removeFileBackupSetting(
        DatabaseConnection conn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        int server = conn.executeIntQuery("select ffr.server from file_backup_settings fbs inner join failover_file_replications ffr on fbs.replication=ffr.pkey where fbs.pkey=?", pkey);
        String accounting=ServerHandler.getBusinessForServer(conn, server);

        conn.executeUpdate("delete from file_backup_settings where pkey=?", pkey);

        // Notify all clients of the update
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.FILE_BACKUP_SETTINGS,
            accounting,
            server,
            false
        );
    }

    public static void setFileBackupSettings(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int pkey,
        String path,
        boolean backupEnabled
    ) throws IOException, SQLException {
        int server = conn.executeIntQuery("select ffr.server from file_backup_settings fbs inner join failover_file_replications ffr on fbs.replication=ffr.pkey where fbs.pkey=?", pkey);
        String accounting = ServerHandler.getBusinessForServer(conn, server);
        BusinessHandler.checkAccessBusiness(conn, source, "setFileBackupSetting", accounting);

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
            accounting,
            server,
            false
        );
    }

    public static int getAOServerForBackupPartition(
        DatabaseConnection conn,
        int pkey
    ) throws IOException, SQLException {
        return conn.executeIntQuery("select ao_server from backup_partitions where pkey=?", pkey);
    }

    public static String getPathForBackupPartition(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeStringQuery(Connection.TRANSACTION_READ_COMMITTED, true, true, "select path from backup_partitions where pkey=?", pkey);
    }

    public static long getBackupPartitionTotalSize(
        DatabaseConnection conn,
        RequestSource source,
        int pkey
    ) throws IOException, SQLException {
        int aoServer=getAOServerForBackupPartition(conn, pkey);
        ServerHandler.checkAccessServer(conn, source, "getBackupPartitionTotalSize", aoServer);
        if(DaemonHandler.isDaemonAvailable(aoServer)) {
            String path=getPathForBackupPartition(conn, pkey);
            try {
                return DaemonHandler.getDaemonConnector(conn, aoServer).getDiskDeviceTotalSize(path);
            } catch(IOException err) {
                DaemonHandler.flagDaemonAsDown(aoServer);
                logger.log(Level.SEVERE, "pkey="+pkey+", path="+path+", aoServer="+aoServer, err);
                return -1;
            } catch(SQLException err) {
                DaemonHandler.flagDaemonAsDown(aoServer);
                logger.log(Level.SEVERE, "pkey="+pkey+", path="+path+", aoServer="+aoServer, err);
                return -1;
            }
        } else return -1;
    }

    public static long getBackupPartitionUsedSize(
        DatabaseConnection conn,
        RequestSource source,
        int pkey
    ) throws IOException, SQLException {
        int aoServer=getAOServerForBackupPartition(conn, pkey);
        ServerHandler.checkAccessServer(conn, source, "getBackupPartitionUsedSize", aoServer);
        if(DaemonHandler.isDaemonAvailable(aoServer)) {
            String path=getPathForBackupPartition(conn, pkey);
            try {
                return DaemonHandler.getDaemonConnector(conn, aoServer).getDiskDeviceUsedSize(path);
            } catch(IOException err) {
                DaemonHandler.flagDaemonAsDown(aoServer);
                logger.log(Level.SEVERE, "pkey="+pkey+", path="+path+", aoServer="+aoServer, err);
                return -1;
            } catch(SQLException err) {
                DaemonHandler.flagDaemonAsDown(aoServer);
                logger.log(Level.SEVERE, "pkey="+pkey+", path="+path+", aoServer="+aoServer, err);
                return -1;
            }
        } else return -1;
    }
}
