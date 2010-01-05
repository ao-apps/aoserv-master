package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.LinuxAccountGroup;
import com.aoindustries.aoserv.client.LinuxAccountGroupService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseLinuxAccountGroupService extends DatabaseServiceIntegerKey<LinuxAccountGroup> implements LinuxAccountGroupService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<LinuxAccountGroup> objectFactory = new AutoObjectFactory<LinuxAccountGroup>(LinuxAccountGroup.class, this);

    DatabaseLinuxAccountGroupService(DatabaseConnector connector) {
        super(connector, LinuxAccountGroup.class);
    }

    protected Set<LinuxAccountGroup> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select pkey, linux_account, linux_group, is_primary from linux_account_groups"
        );
    }

    protected Set<LinuxAccountGroup> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  lag.pkey,\n"
            + "  lag.linux_account,\n"
            + "  lag.linux_group,\n"
            + "  lag.is_primary\n"
            + "from\n"
            + "  master_servers ms\n"
            + "  left join ao_servers ff on ms.server=ff.failover_server,\n"
            + "  linux_account_groups lag\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and (\n"
            + "    ms.server=lag.ao_server\n"
            + "    or ff.server=lag.ao_server\n"
            + "  )",
            connector.getConnectAs()
        );
    }

    protected Set<LinuxAccountGroup> getSetBusiness() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  pkey,\n"
            + "  linux_account,\n"
            + "  linux_group,\n"
            + "  is_primary\n"
            + "from\n"
            + "  linux_account_groups\n"
            + "where\n"
            + "  linux_account in (\n"
            + "    select\n"
            + "      la.ao_server_resource\n"
            + "    from\n"
            + "      usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "      linux_accounts la\n"
            + "    where\n"
            + "      un1.username=?\n"
            + "      and (\n"
            + UN1_BU1_PARENTS_WHERE
            + "      )\n"
            + "      and bu1.accounting=la.accounting\n"
            + "  )\n"
            + "  or linux_group in (\n"
            + "    select\n"
            + "      lg.ao_server_resource\n"
            + "    from\n"
            + "      usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "      linux_groups lg\n"
            + "    where\n"
            + "      un1.username=?\n"
            + "      and (\n"
            + UN1_BU1_PARENTS_WHERE
            + "      )\n"
            + "      and bu1.accounting=lg.accounting\n"
            + "  )",
            connector.getConnectAs(),
            connector.getConnectAs()
        );
    }
}
