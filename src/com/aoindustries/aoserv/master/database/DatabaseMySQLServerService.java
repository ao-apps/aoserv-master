/*
 * Copyright 2009-2011 by AO Industries, Inc.,
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
final class DatabaseMySQLServerService extends DatabaseAOServerResourceService<MySQLServer> implements MySQLServerService {

    private final ObjectFactory<MySQLServer> objectFactory = new AutoObjectFactory<MySQLServer>(MySQLServer.class, connector);

    DatabaseMySQLServerService(DatabaseConnector connector) {
        super(connector, MySQLServer.class);
    }

    @Override
    protected ArrayList<MySQLServer> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<MySQLServer>(),
            objectFactory,
            "select\n"
            + AOSERVER_RESOURCE_SELECT_COLUMNS + ",\n"
            + "  ms.name,\n"
            + "  ms.version,\n"
            + "  ms.max_connections,\n"
            + "  ms.net_bind\n"
            + "from\n"
            + "  mysql_servers ms\n"
            + "  inner join ao_server_resources asr on ms.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey"
        );
    }

    @Override
    protected ArrayList<MySQLServer> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<MySQLServer>(),
            objectFactory,
            "select\n"
            + AOSERVER_RESOURCE_SELECT_COLUMNS + ",\n"
            + "  mys.name,\n"
            + "  mys.version,\n"
            + "  mys.max_connections,\n"
            + "  mys.net_bind\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  mysql_servers mys\n"
            + "  inner join ao_server_resources asr on mys.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=mys.ao_server",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<MySQLServer> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<MySQLServer>(),
            objectFactory,
            "select\n"
            + AOSERVER_RESOURCE_SELECT_COLUMNS + ",\n"
            + "  ms.name,\n"
            + "  ms.version,\n"
            + "  ms.max_connections,\n"
            + "  ms.net_bind\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            + "  mysql_servers ms\n"
            + "  inner join ao_server_resources asr on ms.ao_server_resource=asr.resource\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs.accounting\n"
            + "  and bs.server=ms.ao_server",
            connector.getConnectAs()
        );
    }
}
