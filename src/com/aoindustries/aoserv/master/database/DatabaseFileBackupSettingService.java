package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.FileBackupSetting;
import com.aoindustries.aoserv.client.FileBackupSettingService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import com.aoindustries.util.ArraySet;
import com.aoindustries.util.HashCodeComparator;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseFileBackupSettingService extends DatabaseService<Integer,FileBackupSetting> implements FileBackupSettingService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<FileBackupSetting> objectFactory = new AutoObjectFactory<FileBackupSetting>(FileBackupSetting.class, this);

    DatabaseFileBackupSettingService(DatabaseConnector connector) {
        super(connector, Integer.class, FileBackupSetting.class);
    }

    @Override
    protected Set<FileBackupSetting> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<FileBackupSetting>(HashCodeComparator.getInstance()),
            objectFactory,
            "select * from file_backup_settings order by pkey"
        );
    }

    @Override
    protected Set<FileBackupSetting> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<FileBackupSetting>(HashCodeComparator.getInstance()),
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
            + "  and ffr.pkey=fbs.replication\n"
            + "order by\n"
            + "  fbs.pkey",
            connector.getConnectAs()
        );
    }

    @Override
    protected Set<FileBackupSetting> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<FileBackupSetting>(HashCodeComparator.getInstance()),
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
            + "  and ffr.pkey=fbs.replication\n"
            + "order by\n"
            + "  fbs.pkey",
            connector.getConnectAs()
        );
    }
}
