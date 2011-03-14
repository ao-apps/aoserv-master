/*
 * Copyright 2011 by AO Industries, Inc.,
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
final class DatabaseBackupServerService extends DatabaseServerService<BackupServer> implements BackupServerService {

    private final ObjectFactory<BackupServer> objectFactory = new AutoObjectFactory<BackupServer>(BackupServer.class, connector);

    DatabaseBackupServerService(DatabaseConnector connector) {
        super(connector, BackupServer.class);
    }

    @Override
    protected ArrayList<BackupServer> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<BackupServer>(),
            objectFactory,
            "select\n"
            + SERVER_SELECT_COLUMNS + "\n"
            + "from\n"
            + "  backup_servers bs\n"
            + "  inner join servers se on bs.resource=se.resource\n"
            + "  inner join resources re on se.resource=re.pkey\n"
        );
    }

    @Override
    protected ArrayList<BackupServer> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<BackupServer>(),
            objectFactory,
            "select distinct\n"
            + SERVER_SELECT_COLUMNS + "\n"
            + "from\n"
            + "  master_servers ms\n"
            + "  inner join backup_servers bs on ms.server=bs.resource\n"
            + "  inner join servers se on bs.resource=se.resource\n"
            + "  inner join resources re on se.resource=re.pkey\n"
            + "where\n"
            + "  ms.username=?",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<BackupServer> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<BackupServer>(),
            objectFactory,
            "select distinct\n"
            + SERVER_SELECT_COLUMNS + "\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs2,\n"
            + "  backup_servers bs\n"
            + "  inner join servers se on bs.resource=se.resource\n"
            + "  inner join resources re on se.resource=re.pkey\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs2.accounting\n"
            + "  and bs2.server=bs.resource",
            connector.getConnectAs()
        );
    }
}
