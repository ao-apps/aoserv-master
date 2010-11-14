/*
 * Copyright 2009-2010 by AO Industries, Inc.,
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
final class DatabasePostgresServerService extends DatabaseService<Integer,PostgresServer> implements PostgresServerService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<PostgresServer> objectFactory = new AutoObjectFactory<PostgresServer>(PostgresServer.class, connector);

    DatabasePostgresServerService(DatabaseConnector connector) {
        super(connector, Integer.class, PostgresServer.class);
    }

    @Override
    protected ArrayList<PostgresServer> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<PostgresServer>(),
            objectFactory,
            "select\n"
            + DatabaseAOServerResourceService.SELECT_COLUMNS
            + "  ps.name,\n"
            + "  ps.version,\n"
            + "  ps.max_connections,\n"
            + "  ps.net_bind,\n"
            + "  ps.sort_mem,\n"
            + "  ps.shared_buffers,\n"
            + "  ps.fsync\n"
            + "from\n"
            + "  postgres_servers ps\n"
            + "  inner join ao_server_resources asr on ps.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey"
        );
    }

    @Override
    protected ArrayList<PostgresServer> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<PostgresServer>(),
            objectFactory,
            "select\n"
            + DatabaseAOServerResourceService.SELECT_COLUMNS
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
            + "  inner join ao_server_resources asr on ps.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=ps.ao_server",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<PostgresServer> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<PostgresServer>(),
            objectFactory,
            "select\n"
            + DatabaseAOServerResourceService.SELECT_COLUMNS
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
            + "  inner join ao_server_resources asr on ps.ao_server_resource=asr.resource\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs.accounting\n"
            + "  and bs.server=ps.ao_server",
            connector.getConnectAs()
        );
    }
}
