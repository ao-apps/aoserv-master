package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.LinuxGroup;
import com.aoindustries.aoserv.client.LinuxGroupService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseLinuxGroupService extends DatabaseService<Integer,LinuxGroup> implements LinuxGroupService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<LinuxGroup> objectFactory = new AutoObjectFactory<LinuxGroup>(LinuxGroup.class, this);

    DatabaseLinuxGroupService(DatabaseConnector connector) {
        super(connector, Integer.class, LinuxGroup.class);
    }

    protected Set<LinuxGroup> getSetMaster(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  ao_server_resource,\n"
            + "  linux_group_type,\n"
            + "  group_name,\n"
            + "  gid\n"
            + "from\n"
            + "  linux_groups"
        );
    }

    protected Set<LinuxGroup> getSetDaemon(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  lg.ao_server_resource,\n"
            + "  lg.linux_group_type,\n"
            + "  lg.group_name,\n"
            + "  lg.gid\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  linux_groups lg\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=lg.ao_server",
            connector.getConnectAs()
        );
    }

    protected Set<LinuxGroup> getSetBusiness(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            // Owns group
             "select\n"
            + "  lg.ao_server_resource,\n"
            + "  lg.linux_group_type,\n"
            + "  lg.group_name,\n"
            + "  lg.gid\n"
            + "from\n"
            + "  usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "  linux_groups lg\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and (\n"
            + UN1_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=lg.accounting\n"
            // Has access to server, include mailonly group
            + "union select\n"
            + "  lg.ao_server_resource,\n"
            + "  lg.linux_group_type,\n"
            + "  lg.group_name,\n"
            + "  lg.gid\n"
            + "from\n"
            + "  usernames un\n"
            + "  inner join business_servers bs on un.accounting=bs.accounting\n"
            + "  inner join linux_groups lg on bs.server=lg.ao_server\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and lg.group_name=?",
            connector.getConnectAs(),
            connector.getConnectAs(),
            LinuxGroup.MAILONLY
        );
    }
}
