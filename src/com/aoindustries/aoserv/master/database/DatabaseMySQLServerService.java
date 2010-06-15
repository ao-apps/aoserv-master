package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.MySQLServer;
import com.aoindustries.aoserv.client.MySQLServerService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseMySQLServerService extends DatabaseService<Integer,MySQLServer> implements MySQLServerService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<MySQLServer> objectFactory = new AutoObjectFactory<MySQLServer>(MySQLServer.class, this);

    DatabaseMySQLServerService(DatabaseConnector connector) {
        super(connector, Integer.class, MySQLServer.class);
    }

    protected Set<MySQLServer> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select ao_server_resource, name, version, max_connections, net_bind from mysql_servers"
        );
    }

    protected Set<MySQLServer> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  mys.ao_server_resource,\n"
            + "  mys.name,\n"
            + "  mys.version,\n"
            + "  mys.max_connections,\n"
            + "  mys.net_bind\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  mysql_servers mys\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=mys.ao_server",
            connector.getConnectAs()
        );
    }

    protected Set<MySQLServer> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  ms.ao_server_resource,\n"
            + "  ms.name,\n"
            + "  ms.version,\n"
            + "  ms.max_connections,\n"
            + "  ms.net_bind\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            + "  mysql_servers ms\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs.accounting\n"
            + "  and bs.server=ms.ao_server",
            connector.getConnectAs()
        );
    }
}
