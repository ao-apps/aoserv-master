/*
 * Copyright 2002-2013, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.client.validator.UnixPath;
import com.aoindustries.dbc.DatabaseConnection;
import java.io.IOException;
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
        boolean backupEnabled,
        boolean required
    ) throws IOException, SQLException {
        int server = conn.executeIntQuery("select server from backup.\"FileReplication\" where pkey=?", replication);
        int packageNum = ServerHandler.getPackageForServer(conn, server);
        PackageHandler.checkAccessPackage(conn, source, "addFileBackupSetting", packageNum);

        path=path.trim();
        if(path.length()==0) throw new SQLException("Path may not be empty: "+path);
        int slashPos=path.indexOf('/');
        if(slashPos==-1) throw new SQLException("Path must contain a slash (/): "+path);
        // TODO: Check for windows roots: if(FilePathHandler.getRootNode(backupConn, path.substring(0, slashPos+1))==-1) throw new SQLException("Path does not start with a valid root: "+path);

        int pkey = conn.executeIntUpdate(
            "INSERT INTO backup.\"FileReplicationSetting\" (replication, \"path\", backup_enabled, required) VALUES (?,?,?,?) RETURNING pkey",
            replication,
            path,
            backupEnabled,
            required
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
    }

    public static void removeFileBackupSetting(
        DatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        int server = conn.executeIntQuery("select ffr.server from backup.\"FileReplicationSetting\" fbs inner join backup.\"FileReplication\" ffr on fbs.replication=ffr.pkey where fbs.pkey=?", pkey);
        int packageNum=ServerHandler.getPackageForServer(conn, server);
        PackageHandler.checkAccessPackage(conn, source, "removeFileBackupSetting", packageNum);

        removeFileBackupSetting(conn, invalidateList, pkey);
    }

    public static void removeFileBackupSetting(
        DatabaseConnection conn,
        InvalidateList invalidateList,
        int pkey
    ) throws IOException, SQLException {
        int server = conn.executeIntQuery("select ffr.server from backup.\"FileReplicationSetting\" fbs inner join backup.\"FileReplication\" ffr on fbs.replication=ffr.pkey where fbs.pkey=?", pkey);
        int packageNum=ServerHandler.getPackageForServer(conn, server);

        conn.executeUpdate("delete from backup.\"FileReplicationSetting\" where pkey=?", pkey);

        // Notify all clients of the update
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.FILE_BACKUP_SETTINGS,
            PackageHandler.getBusinessForPackage(conn, packageNum),
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
        boolean backupEnabled,
        boolean required
    ) throws IOException, SQLException {
        int server = conn.executeIntQuery("select ffr.server from backup.\"FileReplicationSetting\" fbs inner join backup.\"FileReplication\" ffr on fbs.replication=ffr.pkey where fbs.pkey=?", pkey);
        int packageNum = ServerHandler.getPackageForServer(conn, server);
        PackageHandler.checkAccessPackage(conn, source, "setFileBackupSetting", packageNum);

        path=path.trim();
        if(path.length()==0) throw new SQLException("Path may not be empty: "+path);
        int slashPos=path.indexOf('/');
        if(slashPos==-1) throw new SQLException("Path must contain a slash (/): "+path);
        // TODO: Check for windows roots: if(FilePathHandler.getRootNode(backupConn, path.substring(0, slashPos+1))==-1) throw new SQLException("Path does not start with a valid root: "+path);

        conn.executeUpdate(
            "update\n"
            + "  backup.\"FileReplicationSetting\"\n"
            + "set\n"
            + "  path=?,\n"
            + "  backup_enabled=?,\n"
            + "  required=?\n"
            + "where\n"
            + "  pkey=?",
            path,
            backupEnabled,
            required,
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
    }

    public static int getAOServerForBackupPartition(
        DatabaseConnection conn,
        int pkey
    ) throws IOException, SQLException {
        return conn.executeIntQuery("select ao_server from backup.\"BackupPartition\" where pkey=?", pkey);
    }

    public static UnixPath getPathForBackupPartition(DatabaseConnection conn, int pkey) throws IOException, SQLException {
		return conn.executeObjectQuery(
			ObjectFactories.unixPathFactory,
			"select path from backup.\"BackupPartition\" where pkey=?",
			pkey
		);
    }

    public static long getBackupPartitionTotalSize(
        DatabaseConnection conn,
        RequestSource source,
        int pkey
    ) throws IOException, SQLException {
        int aoServer=getAOServerForBackupPartition(conn, pkey);
        ServerHandler.checkAccessServer(conn, source, "getBackupPartitionTotalSize", aoServer);
        if(DaemonHandler.isDaemonAvailable(aoServer)) {
            UnixPath path=getPathForBackupPartition(conn, pkey);
            try {
                return DaemonHandler.getDaemonConnector(conn, aoServer).getDiskDeviceTotalSize(path);
            } catch(IOException | SQLException err) {
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
            UnixPath path=getPathForBackupPartition(conn, pkey);
            try {
                return DaemonHandler.getDaemonConnector(conn, aoServer).getDiskDeviceUsedSize(path);
            } catch(IOException | SQLException err) {
                DaemonHandler.flagDaemonAsDown(aoServer);
                logger.log(Level.SEVERE, "pkey="+pkey+", path="+path+", aoServer="+aoServer, err);
                return -1;
            }
        } else return -1;
    }
}
