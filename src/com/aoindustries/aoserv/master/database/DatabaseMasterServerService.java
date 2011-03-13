/*
 * Copyright 2010-2011 by AO Industries, Inc.,
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
final class DatabaseMasterServerService extends DatabaseService<Integer,MasterServer> implements MasterServerService {

    private final ObjectFactory<MasterServer> objectFactory = new AutoObjectFactory<MasterServer>(MasterServer.class, connector);

    DatabaseMasterServerService(DatabaseConnector connector) {
        super(connector, Integer.class, MasterServer.class);
    }

    @Override
    protected ArrayList<MasterServer> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<MasterServer>(),
            objectFactory,
            "select * from master_servers"
        );
    }

    @Override
    protected ArrayList<MasterServer> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<MasterServer>(),
            objectFactory,
            "select\n"
            + "  ms2.*\n"
            + "from\n"
            + "  master_servers ms1,\n"
            + "  master_servers ms2\n"
            + "where\n"
            + "  ms1.username=?\n"
            + "  and ms1.server=ms2.server",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<MasterServer> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<MasterServer>(),
            objectFactory,
            "select\n"
            + "  ms.*\n"
            + "from\n"
            + "  usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "  usernames un2,\n"
            + "  master_servers ms\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and (\n"
            + "    un2.username=un1.username\n"
            + UN1_BU1_PARENTS_OR_WHERE
            + "  )\n"
            + "  and bu1.accounting=un2.accounting\n"
            + "  and un2.username=ms.username",
            connector.getConnectAs()
        );
    }
}
