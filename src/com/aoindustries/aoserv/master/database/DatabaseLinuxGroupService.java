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
final class DatabaseLinuxGroupService extends DatabaseAOServerResourceService<LinuxGroup> implements LinuxGroupService {

    private final ObjectFactory<LinuxGroup> objectFactory = new AutoObjectFactory<LinuxGroup>(LinuxGroup.class, connector);

    DatabaseLinuxGroupService(DatabaseConnector connector) {
        super(connector, LinuxGroup.class);
    }

    @Override
    protected ArrayList<LinuxGroup> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<LinuxGroup>(),
            objectFactory,
            "select\n"
            + AOSERVER_RESOURCE_SELECT_COLUMNS + ",\n"
            + "  lg.linux_group_type,\n"
            + "  lg.group_name,\n"
            + "  lg.gid\n"
            + "from\n"
            + "  linux_groups lg\n"
            + "  inner join ao_server_resources asr on lg.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey"
        );
    }

    @Override
    protected ArrayList<LinuxGroup> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<LinuxGroup>(),
            objectFactory,
            "select\n"
            + AOSERVER_RESOURCE_SELECT_COLUMNS + ",\n"
            + "  lg.linux_group_type,\n"
            + "  lg.group_name,\n"
            + "  lg.gid\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  linux_groups lg\n"
            + "  inner join ao_server_resources asr on lg.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=lg.ao_server",
            connector.getSwitchUser()
        );
    }

    @Override
    protected ArrayList<LinuxGroup> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<LinuxGroup>(),
            objectFactory,
            // Owns group
             "select\n"
            + AOSERVER_RESOURCE_SELECT_COLUMNS + ",\n"
            + "  lg.linux_group_type,\n"
            + "  lg.group_name,\n"
            + "  lg.gid\n"
            + "from\n"
            + "  usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "  linux_groups lg\n"
            + "  inner join ao_server_resources asr on lg.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and (\n"
            + UN1_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=lg.accounting\n"
            // Has access to server, include mailonly group
            + "union select\n"
            + AOSERVER_RESOURCE_SELECT_COLUMNS + ",\n"
            + "  lg.linux_group_type,\n"
            + "  lg.group_name,\n"
            + "  lg.gid\n"
            + "from\n"
            + "  usernames un\n"
            + "  inner join business_servers bs on un.accounting=bs.accounting\n"
            + "  inner join linux_groups lg on bs.server=lg.ao_server\n"
            + "  inner join ao_server_resources asr on lg.ao_server_resource=asr.resource\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and lg.group_name=?",
            connector.getSwitchUser(),
            connector.getSwitchUser(),
            LinuxGroup.MAILONLY
        );
    }
}
