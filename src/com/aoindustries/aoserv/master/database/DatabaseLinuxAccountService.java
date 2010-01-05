package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServObject;
import com.aoindustries.aoserv.client.LinuxAccount;
import com.aoindustries.aoserv.client.LinuxAccountService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseLinuxAccountService extends DatabaseServiceIntegerKey<LinuxAccount> implements LinuxAccountService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<LinuxAccount> objectFactory = new AutoObjectFactory<LinuxAccount>(LinuxAccount.class, this);

    DatabaseLinuxAccountService(DatabaseConnector connector) {
        super(connector, LinuxAccount.class);
    }

    protected Set<LinuxAccount> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  ao_server_resource,\n"
            + "  linux_account_type,\n"
            + "  username,\n"
            + "  uid,\n"
            + "  home,\n"
            + "  name,\n"
            + "  office_location,\n"
            + "  office_phone,\n"
            + "  home_phone,\n"
            + "  shell,\n"
            + "  predisable_password\n"
            + "from\n"
            + "  linux_accounts"
        );
    }

    protected Set<LinuxAccount> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  la.ao_server_resource,\n"
            + "  la.linux_account_type,\n"
            + "  la.username,\n"
            + "  la.uid,\n"
            + "  la.home,\n"
            + "  la.name,\n"
            + "  la.office_location,\n"
            + "  la.office_phone,\n"
            + "  la.home_phone,\n"
            + "  la.shell,\n"
            + "  la.predisable_password\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  linux_accounts la\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=la.ao_server",
            connector.getConnectAs()
        );
    }

    protected Set<LinuxAccount> getSetBusiness() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
             "select\n"
            + "  la.ao_server_resource,\n"
            + "  la.linux_account_type,\n"
            + "  la.username,\n"
            + "  la.uid,\n"
            + "  la.home,\n"
            + "  la.name,\n"
            + "  la.office_location,\n"
            + "  la.office_phone,\n"
            + "  la.home_phone,\n"
            + "  la.shell,\n"
            + "  case when la.predisable_password is null then null else ? end\n"
            + "from\n"
            + "  usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "  linux_accounts la\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and (\n"
            + UN1_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=la.accounting",
            AOServObject.FILTERED,
            connector.getConnectAs()
        );
    }
}