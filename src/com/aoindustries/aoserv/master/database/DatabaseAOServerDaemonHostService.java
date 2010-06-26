package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServerDaemonHost;
import com.aoindustries.aoserv.client.AOServerDaemonHostService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import com.aoindustries.util.ArraySet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseAOServerDaemonHostService extends DatabaseService<Integer,AOServerDaemonHost> implements AOServerDaemonHostService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<AOServerDaemonHost> objectFactory = new AutoObjectFactory<AOServerDaemonHost>(AOServerDaemonHost.class, this);

    DatabaseAOServerDaemonHostService(DatabaseConnector connector) {
        super(connector, Integer.class, AOServerDaemonHost.class);
    }

    @Override
    protected Set<AOServerDaemonHost> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<AOServerDaemonHost>(),
            objectFactory,
            "select * from ao_server_daemon_hosts order by pkey"
        );
    }

    @Override
    protected Set<AOServerDaemonHost> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<AOServerDaemonHost>(),
            objectFactory,
            "select\n"
            + "  sdh.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  ao_server_daemon_hosts sdh\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=sdh.ao_server\n"
            + "order by\n"
            + "  pkey",
            connector.getConnectAs()
        );
    }

    @Override
    protected Set<AOServerDaemonHost> getSetBusiness(DatabaseConnection db) {
        return Collections.emptySet();
    }
}
