/*
 * Copyright 2002-2013, 2015, 2017, 2018, 2019, 2020 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.linux.PosixPath;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.daemon.client.AOServDaemonConnector;
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

    private static final Logger logger = Logger.getLogger(BackupHandler.class.getName());

    private BackupHandler() {
    }

    public static int addFileReplicationSetting(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int fileReplication,
        String path,
        boolean backupEnabled,
        boolean required
    ) throws IOException, SQLException {
        int host = conn.executeIntQuery("select server from backup.\"FileReplication\" where id=?", fileReplication);
        int packageNum = NetHostHandler.getPackageForHost(conn, host);
        PackageHandler.checkAccessPackage(conn, source, "addFileReplicationSetting", packageNum);

        path=path.trim();
        if(path.length()==0) throw new SQLException("Path may not be empty: "+path);
        int slashPos=path.indexOf('/');
        if(slashPos==-1) throw new SQLException("Path must contain a slash (/): "+path);
        // TODO: Check for windows roots: if(FilePathHandler.getRootNode(backupConn, path.substring(0, slashPos+1))==-1) throw new SQLException("Path does not start with a valid root: "+path);

        int fileReplicationSetting = conn.executeIntUpdate(
            "INSERT INTO backup.\"FileReplicationSetting\" (replication, \"path\", backup_enabled, required) VALUES (?,?,?,?) RETURNING id",
            fileReplication,
            path,
            backupEnabled,
            required
        );

        // Notify all clients of the update
        invalidateList.addTable(
            conn,
            Table.TableID.FILE_BACKUP_SETTINGS,
            PackageHandler.getAccountForPackage(conn, packageNum),
            host,
            false
        );
        return fileReplicationSetting;
    }

    public static void removeFileReplicationSetting(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int fileReplicationSetting
    ) throws IOException, SQLException {
        int host = conn.executeIntQuery("select ffr.server from backup.\"FileReplicationSetting\" fbs inner join backup.\"FileReplication\" ffr on fbs.replication=ffr.id where fbs.id=?", fileReplicationSetting);
        int packageNum=NetHostHandler.getPackageForHost(conn, host);
        PackageHandler.checkAccessPackage(conn, source, "removeFileReplicationSetting", packageNum);

        removeFileReplicationSetting(conn, invalidateList, fileReplicationSetting);
    }

    public static void removeFileReplicationSetting(
        DatabaseConnection conn,
        InvalidateList invalidateList,
        int fileReplicationSetting
    ) throws IOException, SQLException {
        int host = conn.executeIntQuery("select ffr.server from backup.\"FileReplicationSetting\" fbs inner join backup.\"FileReplication\" ffr on fbs.replication=ffr.id where fbs.id=?", fileReplicationSetting);
        int packageNum = NetHostHandler.getPackageForHost(conn, host);

        conn.executeUpdate("delete from backup.\"FileReplicationSetting\" where id=?", fileReplicationSetting);

        // Notify all clients of the update
        invalidateList.addTable(
            conn,
            Table.TableID.FILE_BACKUP_SETTINGS,
            PackageHandler.getAccountForPackage(conn, packageNum),
            host,
            false
        );
    }

    public static void setFileReplicationSettings(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int fileReplicationSetting,
        String path,
        boolean backupEnabled,
        boolean required
    ) throws IOException, SQLException {
        int host = conn.executeIntQuery("select ffr.server from backup.\"FileReplicationSetting\" fbs inner join backup.\"FileReplication\" ffr on fbs.replication=ffr.id where fbs.id=?", fileReplicationSetting);
        int packageNum = NetHostHandler.getPackageForHost(conn, host);
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
            + "  id=?",
            path,
            backupEnabled,
            required,
            fileReplicationSetting
        );

        // Notify all clients of the update
        invalidateList.addTable(
            conn,
            Table.TableID.FILE_BACKUP_SETTINGS,
            PackageHandler.getAccountForPackage(conn, packageNum),
            host,
            false
        );
    }

    public static int getLinuxServerForBackupPartition(
        DatabaseConnection conn,
        int backupPartition
    ) throws IOException, SQLException {
        return conn.executeIntQuery("select ao_server from backup.\"BackupPartition\" where id=?", backupPartition);
    }

    public static PosixPath getPathForBackupPartition(DatabaseConnection conn, int backupPartition) throws IOException, SQLException {
		return conn.executeObjectQuery(ObjectFactories.posixPathFactory,
			"select path from backup.\"BackupPartition\" where id=?",
			backupPartition
		);
    }

    public static long getBackupPartitionTotalSize(
        DatabaseConnection conn,
        RequestSource source,
        int backupPartition
    ) throws IOException, SQLException {
        int linuxServer = getLinuxServerForBackupPartition(conn, backupPartition);
        NetHostHandler.checkAccessHost(conn, source, "getBackupPartitionTotalSize", linuxServer);
        if(DaemonHandler.isDaemonAvailable(linuxServer)) {
            PosixPath path=getPathForBackupPartition(conn, backupPartition);
            try {
				AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
				conn.releaseConnection();
                return daemonConnector.getDiskDeviceTotalSize(path);
            } catch(IOException | SQLException err) {
                DaemonHandler.flagDaemonAsDown(linuxServer);
                logger.log(Level.SEVERE, "id="+backupPartition+", path="+path+", linuxServer="+linuxServer, err);
                return -1;
            }
        } else return -1;
    }

    public static long getBackupPartitionUsedSize(
        DatabaseConnection conn,
        RequestSource source,
        int backupPartition
    ) throws IOException, SQLException {
        int linuxServer = getLinuxServerForBackupPartition(conn, backupPartition);
        NetHostHandler.checkAccessHost(conn, source, "getBackupPartitionUsedSize", linuxServer);
        if(DaemonHandler.isDaemonAvailable(linuxServer)) {
            PosixPath path=getPathForBackupPartition(conn, backupPartition);
            try {
				AOServDaemonConnector daemonConnector = DaemonHandler.getDaemonConnector(conn, linuxServer);
				conn.releaseConnection();
                return daemonConnector.getDiskDeviceUsedSize(path);
            } catch(IOException | SQLException err) {
                DaemonHandler.flagDaemonAsDown(linuxServer);
                logger.log(Level.SEVERE, "id="+backupPartition+", path="+path+", linuxServer="+linuxServer, err);
                return -1;
            }
        } else return -1;
    }
}
