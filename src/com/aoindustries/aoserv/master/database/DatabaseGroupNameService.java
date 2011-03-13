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

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseGroupNameService extends DatabaseService<GroupId,GroupName> implements GroupNameService {

    private final ObjectFactory<GroupName> objectFactory = new AutoObjectFactory<GroupName>(GroupName.class, connector);

    DatabaseGroupNameService(DatabaseConnector connector) {
        super(connector, GroupId.class, GroupName.class);
    }

    @Override
    protected ArrayList<GroupName> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<GroupName>(),
            objectFactory,
            "select * from group_names"
        );
    }

    @Override
    protected ArrayList<GroupName> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<GroupName>(),
            objectFactory,
            "select distinct\n"
            + "  gn.*\n"
            + "from\n"
            + "  master_servers ms\n"
            + "  left join ao_servers ff on ms.server=ff.failover_server,\n"
            + "  business_servers bs,\n"
            + "  group_names gn\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and (\n"
            + "    ms.server=bs.server\n"
            + "    or ff.server=bs.server\n"
            + "  ) and bs.accounting=gn.accounting",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<GroupName> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<GroupName>(),
            objectFactory,
            "select\n"
            + "  gn.*\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  group_names gn\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  ) and bu1.accounting=gn.accounting\n"
            + "union select\n"
            + "  gn.*\n"
            + "from\n"
            + "  group_names gn\n"
            + "where\n"
            + "  gn.group_name=?",
            connector.getConnectAs(),
            LinuxGroup.MAILONLY
        );
    }
}
