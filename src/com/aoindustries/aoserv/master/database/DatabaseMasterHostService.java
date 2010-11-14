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
final class DatabaseMasterHostService extends DatabaseService<Integer,MasterHost> implements MasterHostService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<MasterHost> objectFactory = new AutoObjectFactory<MasterHost>(MasterHost.class, connector);

    DatabaseMasterHostService(DatabaseConnector connector) {
        super(connector, Integer.class, MasterHost.class);
    }

    @Override
    protected ArrayList<MasterHost> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<MasterHost>(),
            objectFactory,
            "select * from master_hosts"
        );
    }

    @Override
    protected ArrayList<MasterHost> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<MasterHost>(),
            objectFactory,
            "select distinct\n"
            + "  mh.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  business_servers bs,\n"
            + "  usernames un,\n"
            + "  master_hosts mh\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=bs.server\n"
            + "  and bs.accounting=un.accounting\n"
            + "  and un.username=mh.username",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<MasterHost> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<MasterHost>(),
            objectFactory,
            "select\n"
            + "  mh.*\n"
            + "from\n"
            + "  usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "  usernames un2,\n"
            + "  master_hosts mh\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and (\n"
            + "    un2.username=un1.username\n"
            + UN1_BU1_PARENTS_OR_WHERE
            + "  )\n"
            + "  and bu1.accounting=un2.accounting\n"
            + "  and un2.username=mh.username",
            connector.getConnectAs()
        );
    }
}
