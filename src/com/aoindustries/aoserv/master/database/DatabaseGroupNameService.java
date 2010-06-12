package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.GroupName;
import com.aoindustries.aoserv.client.GroupNameService;
import com.aoindustries.aoserv.client.LinuxGroup;
import com.aoindustries.aoserv.client.validator.GroupId;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseGroupNameService extends DatabaseService<GroupId,GroupName> implements GroupNameService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<GroupName> objectFactory = new AutoObjectFactory<GroupName>(GroupName.class, this);

    DatabaseGroupNameService(DatabaseConnector connector) {
        super(connector, GroupId.class, GroupName.class);
    }

    protected Set<GroupName> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from group_names"
        );
    }

    protected Set<GroupName> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
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
            connector.connectAs
        );
    }

    protected Set<GroupName> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
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
            + "  ) and (\n"
            + "    bu1.accounting=gn.accounting\n"
            + "    or gn.group_name=?\n"
            + "  )",
            connector.connectAs,
            LinuxGroup.MAILONLY
        );
    }
}
