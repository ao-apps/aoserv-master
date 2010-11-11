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
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseHttpdServerService extends DatabaseService<Integer,HttpdServer> implements HttpdServerService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<HttpdServer> objectFactory = new AutoObjectFactory<HttpdServer>(HttpdServer.class, this);

    DatabaseHttpdServerService(DatabaseConnector connector) {
        super(connector, Integer.class, HttpdServer.class);
    }

    @Override
    protected Set<HttpdServer> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<HttpdServer>(),
            objectFactory,
            "select\n"
            + "  ao_server_resource,\n"
            + "  number,\n"
            + "  max_binds,\n"
            + "  linux_account_group,\n"
            + "  mod_php_version,\n"
            + "  use_suexec,\n"
            + "  is_shared,\n"
            + "  use_mod_perl,\n"
            + "  timeout\n"
            + "from\n"
            + "  httpd_servers"
        );
    }

    @Override
    protected Set<HttpdServer> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<HttpdServer>(),
            objectFactory,
            "select\n"
            + "  hs.ao_server_resource,\n"
            + "  hs.number,\n"
            + "  hs.max_binds,\n"
            + "  hs.linux_account_group,\n"
            + "  hs.mod_php_version,\n"
            + "  hs.use_suexec,\n"
            + "  hs.is_shared,\n"
            + "  hs.use_mod_perl,\n"
            + "  hs.timeout\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  httpd_servers hs\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=hs.ao_server",
            connector.getConnectAs()
        );
    }

    @Override
    protected Set<HttpdServer> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<HttpdServer>(),
            objectFactory,
            "select\n"
            + "  hs.ao_server_resource,\n"
            + "  hs.number,\n"
            + "  hs.max_binds,\n"
            + "  hs.linux_account_group,\n"
            + "  hs.mod_php_version,\n"
            + "  hs.use_suexec,\n"
            + "  hs.is_shared,\n"
            + "  hs.use_mod_perl,\n"
            + "  hs.timeout\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            + "  httpd_servers hs\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs.accounting\n"
            + "  and bs.server=hs.ao_server",
            connector.getConnectAs()
        );
    }
}
