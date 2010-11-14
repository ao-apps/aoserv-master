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
final class DatabaseAOServerDaemonHostService extends DatabaseService<Integer,AOServerDaemonHost> implements AOServerDaemonHostService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<AOServerDaemonHost> objectFactory = new AutoObjectFactory<AOServerDaemonHost>(AOServerDaemonHost.class, connector);

    DatabaseAOServerDaemonHostService(DatabaseConnector connector) {
        super(connector, Integer.class, AOServerDaemonHost.class);
    }

    @Override
    protected ArrayList<AOServerDaemonHost> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<AOServerDaemonHost>(),
            objectFactory,
            "select * from ao_server_daemon_hosts"
        );
    }

    @Override
    protected ArrayList<AOServerDaemonHost> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<AOServerDaemonHost>(),
            objectFactory,
            "select\n"
            + "  sdh.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  ao_server_daemon_hosts sdh\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=sdh.ao_server",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<AOServerDaemonHost> getListBusiness(DatabaseConnection db) {
        return new ArrayList<AOServerDaemonHost>(0);
    }
}
