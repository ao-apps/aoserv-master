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
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseBusinessServerService extends DatabaseService<Integer,BusinessServer> implements BusinessServerService {

    private final ObjectFactory<BusinessServer> objectFactory = new AutoObjectFactory<BusinessServer>(BusinessServer.class, connector);

    DatabaseBusinessServerService(DatabaseConnector connector) {
        super(connector, Integer.class, BusinessServer.class);
    }

    @Override
    protected ArrayList<BusinessServer> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<BusinessServer>(),
            objectFactory,
            "select * from business_servers"
        );
    }

    @Override
    protected ArrayList<BusinessServer> getListDaemon(DatabaseConnection db) throws SQLException, RemoteException {
        StringBuilder sql = new StringBuilder(
            "select distinct\n"
            + "  bs.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  business_servers bs\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=bs.server\n"
        );
        // Extra net_binds
        addOptionalInInteger(
            sql,
            "union select\n"
            + "  bs.*\n"
            + "from\n"
            + "  net_binds nb\n"
            + "  inner join business_servers bs on nb.business_server=bs.pkey\n"
            + "where\n"
            + "  nb.pkey in (",
            connector.netBinds.getListDaemon(db),
            ")\n"
        );
        // Extra server_resources
        addOptionalInInteger(
            sql,
            "union select\n"
            + "  bs.*\n"
            + "from\n"
            + "  server_resources sr\n"
            + "  inner join business_servers bs on sr.accounting=bs.accounting and sr.server=bs.server\n"
            + "where\n"
            + "  sr.resource in (",
            connector.serverResources.getSet(),
            ")\n"
        );
        return db.executeObjectCollectionQuery(
            new ArrayList<BusinessServer>(),
            objectFactory,
            sql.toString(),
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<BusinessServer> getListBusiness(DatabaseConnection db) throws SQLException, RemoteException {
        // owns the resource
        StringBuilder sql = new StringBuilder(
            "select\n"
            + "  bs.*\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  business_servers bs\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  ) and bu1.accounting=bs.accounting\n"
        );
        // Extra net_binds
        addOptionalInInteger(
            sql,
            "union select\n"
            + "  bs.*\n"
            + "from\n"
            + "  net_binds nb\n"
            + "  inner join business_servers bs on nb.business_server=bs.pkey\n"
            + "where\n"
            + "  nb.pkey in (",
            connector.netBinds.getListBusiness(db),
            ")\n"
        );
        // Extra server_resources
        addOptionalInInteger(
            sql,
            "union select\n"
            + "  bs.*\n"
            + "from\n"
            + "  server_resources sr\n"
            + "  inner join business_servers bs on sr.accounting=bs.accounting and sr.server=bs.server\n"
            + "where\n"
            + "  sr.resource in (",
            connector.serverResources.getSet(),
            ")\n"
        );
        // Extra ao_server_resources
        addOptionalInInteger(
            sql,
            "union select\n"
            + "  bs.*\n"
            + "from\n"
            + "  ao_server_resources asr\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "where\n"
            + "  asr.resource in (",
            connector.aoserverResources.getSet(),
            ")\n"
        );
        return db.executeObjectCollectionQuery(
            new ArrayList<BusinessServer>(),
            objectFactory,
            sql.toString(),
            connector.getConnectAs()
        );
    }
}
