/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.client.validator.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseMasterUserService extends DatabaseService<UserId,MasterUser> implements MasterUserService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<MasterUser> objectFactory = new AutoObjectFactory<MasterUser>(MasterUser.class, this);

    DatabaseMasterUserService(DatabaseConnector connector) {
        super(connector, UserId.class, MasterUser.class);
    }

    @Override
    protected Set<MasterUser> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<MasterUser>(),
            objectFactory,
            "select * from master_users"
        );
    }

    @Override
    protected Set<MasterUser> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<MasterUser>(),
            objectFactory,
            "select distinct\n"
            + "  mu.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  business_servers bs,\n"
            + "  usernames un,\n"
            + "  master_users mu\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=bs.server\n"
            + "  and bs.accounting=un.accounting\n"
            + "  and un.username=mu.username",
            connector.getConnectAs()
        );
    }

    @Override
    protected Set<MasterUser> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<MasterUser>(),
            objectFactory,
            "select\n"
            + "  mu.*\n"
            + "from\n"
            + "  usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "  usernames un2,\n"
            + "  master_users mu\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and (\n"
            + "    un2.username=un1.username\n"
            + UN1_BU1_PARENTS_OR_WHERE
            + "  )\n"
            + "  and bu1.accounting=un2.accounting\n"
            + "  and un2.username=mu.username",
            connector.getConnectAs()
        );
    }
}
