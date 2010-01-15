package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.PostgresServer;
import com.aoindustries.aoserv.client.PostgresServerService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabasePostgresServerService extends DatabaseService<Integer,PostgresServer> implements PostgresServerService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<PostgresServer> objectFactory = new AutoObjectFactory<PostgresServer>(PostgresServer.class, this);

    DatabasePostgresServerService(DatabaseConnector connector) {
        super(connector, Integer.class, PostgresServer.class);
    }

    protected Set<PostgresServer> getSetMaster(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  ao_server_resource,\n"
            + "  name,\n"
            + "  version,\n"
            + "  max_connections,\n"
            + "  net_bind,\n"
            + "  sort_mem,\n"
            + "  shared_buffers,\n"
            + "  fsync\n"
            + "from\n"
            + "  postgres_servers"
        );
    }

    protected Set<PostgresServer> getSetDaemon(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  ps.ao_server_resource,\n"
            + "  ps.name,\n"
            + "  ps.version,\n"
            + "  ps.max_connections,\n"
            + "  ps.net_bind,\n"
            + "  ps.sort_mem,\n"
            + "  ps.shared_buffers,\n"
            + "  ps.fsync\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  postgres_servers ps\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=ps.ao_server",
            connector.getConnectAs()
        );
    }

    protected Set<PostgresServer> getSetBusiness(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  ps.ao_server_resource,\n"
            + "  ps.name,\n"
            + "  ps.version,\n"
            + "  ps.max_connections,\n"
            + "  ps.net_bind,\n"
            + "  ps.sort_mem,\n"
            + "  ps.shared_buffers,\n"
            + "  ps.fsync\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            + "  postgres_servers ps\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs.accounting\n"
            + "  and bs.server=ps.ao_server",
            connector.getConnectAs()
        );
    }
}
