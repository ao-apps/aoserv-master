package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.BusinessServer;
import com.aoindustries.aoserv.client.BusinessServerService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseBusinessServerService extends DatabaseServiceIntegerKey<BusinessServer> implements BusinessServerService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<BusinessServer> objectFactory = new AutoObjectFactory<BusinessServer>(BusinessServer.class, this);

    DatabaseBusinessServerService(DatabaseConnector connector) {
        super(connector, BusinessServer.class);
    }

    protected Set<BusinessServer> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from business_servers"
        );
    }

    protected Set<BusinessServer> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select distinct\n"
            + "  bs.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  business_servers bs\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=bs.server",
            connector.getConnectAs()
        );
    }

    protected Set<BusinessServer> getSetBusiness() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  bs.*\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  business_servers bs\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=bs.accounting",
            connector.getConnectAs()
        );
    }
}
