package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServerDaemonHost;
import com.aoindustries.aoserv.client.AOServerDaemonHostService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseAOServerDaemonHostService extends DatabaseServiceIntegerKey<AOServerDaemonHost> implements AOServerDaemonHostService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<AOServerDaemonHost> objectFactory = new AutoObjectFactory<AOServerDaemonHost>(AOServerDaemonHost.class, this);

    DatabaseAOServerDaemonHostService(DatabaseConnector connector) {
        super(connector, AOServerDaemonHost.class);
    }

    protected Set<AOServerDaemonHost> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from ao_server_daemon_hosts"
        );
    }

    protected Set<AOServerDaemonHost> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
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

    protected Set<AOServerDaemonHost> getSetBusiness() {
        return Collections.emptySet();
    }
}
