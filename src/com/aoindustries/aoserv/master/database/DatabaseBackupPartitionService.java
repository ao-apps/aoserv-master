package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.BackupPartition;
import com.aoindustries.aoserv.client.BackupPartitionService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseBackupPartitionService extends DatabaseService<Integer,BackupPartition> implements BackupPartitionService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<BackupPartition> objectFactory = new AutoObjectFactory<BackupPartition>(BackupPartition.class, this);

    DatabaseBackupPartitionService(DatabaseConnector connector) {
        super(connector, Integer.class, BackupPartition.class);
    }

    protected Set<BackupPartition> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from backup_partitions"
        );
    }

    protected Set<BackupPartition> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  bp.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  backup_partitions bp\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and (\n"
            + "    ms.server=bp.ao_server\n"
            + "    or (\n"
            + "      select\n"
            + "        ffr.pkey\n"
            + "      from\n"
            + "        failover_file_replications ffr\n"
            + "        inner join backup_partitions bp2 on ffr.backup_partition=bp2.pkey\n"
            + "      where\n"
            + "        ms.server=ffr.server\n"
            + "        and bp.ao_server=bp2.ao_server\n"
            + "      limit 1\n"
            + "    ) is not null\n"
            + "  )",
            connector.getConnectAs()
        );
    }

    protected Set<BackupPartition> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select distinct\n"
            + "  bp.*\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            + "  backup_partitions bp\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs.accounting\n"
            + "  and (\n"
            + "    bs.server=bp.ao_server\n"
            //+ "    or (\n"
            //+ "      select\n"
            //+ "        ffr.pkey\n"
            //+ "      from\n"
            //+ "        failover_file_replications ffr\n"
            //+ "        inner join backup_partitions bp2 on ffr.backup_partition=bp2.pkey\n"
            //+ "      where\n"
            //+ "        bs.server=ffr.server\n"
            //+ "        and bp.ao_server=bp2.ao_server\n"
            //+ "      limit 1\n"
            //+ "    ) is not null\n"
            + "  )",
            connector.getConnectAs()
        );
    }
}
