/*
 * Copyright 2010-2011 by AO Industries, Inc.,
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
import java.util.ArrayList;
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseMasterUserService extends DatabaseAccountTypeService<UserId,MasterUser> implements MasterUserService {

    private final ObjectFactory<MasterUser> objectFactory = new AutoObjectFactory<MasterUser>(MasterUser.class, connector);

    DatabaseMasterUserService(DatabaseConnector connector) {
        super(connector, UserId.class, MasterUser.class);
    }

    @Override
    protected List<MasterUser> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<MasterUser>(),
            objectFactory,
            "select * from master_users"
        );
    }

    @Override
    protected List<MasterUser> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<MasterUser>(),
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
            connector.getSwitchUser()
        );
    }

    @Override
    protected List<MasterUser> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<MasterUser>(),
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
            connector.getSwitchUser()
        );
    }
}
