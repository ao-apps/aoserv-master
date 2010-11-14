/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseFileBackupSettingService extends DatabaseService<Integer,FileBackupSetting> implements FileBackupSettingService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<FileBackupSetting> objectFactory = new AutoObjectFactory<FileBackupSetting>(FileBackupSetting.class, connector);

    DatabaseFileBackupSettingService(DatabaseConnector connector) {
        super(connector, Integer.class, FileBackupSetting.class);
    }

    @Override
    protected ArrayList<FileBackupSetting> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<FileBackupSetting>(),
            objectFactory,
            "select * from file_backup_settings"
        );
    }

    @Override
    protected ArrayList<FileBackupSetting> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<FileBackupSetting>(),
            objectFactory,
            "select\n"
            + "  fbs.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  failover_file_replications ffr,\n"
            + "  file_backup_settings fbs\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=ffr.server\n"
            + "  and ffr.pkey=fbs.replication",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<FileBackupSetting> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<FileBackupSetting>(),
            objectFactory,
            "select\n"
            + "  fbs.*\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  servers se,\n"
            + "  failover_file_replications ffr,\n"
            + "  file_backup_settings fbs\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=se.accounting\n"
            + "  and se.pkey=ffr.server\n"
            + "  and ffr.pkey=fbs.replication",
            connector.getConnectAs()
        );
    }
}
