package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.ServerResource;
import com.aoindustries.aoserv.client.ServerResourceService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseServerResourceService extends DatabaseServiceIntegerKey<ServerResource> implements ServerResourceService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<ServerResource> objectFactory = new AutoObjectFactory<ServerResource>(ServerResource.class, this);

    DatabaseServerResourceService(DatabaseConnector connector) {
        super(connector, ServerResource.class);
    }

    protected Set<ServerResource> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  sr.resource,\n"
            + "  sr.server,\n"
            + "  bs.pkey\n"
            + "from\n"
            + "  server_resources sr\n"
            + "  inner join business_servers bs on sr.accounting=bs.accounting and sr.server=bs.server"
        );
    }

    protected Set<ServerResource> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  sr.resource,\n"
            + "  sr.server,\n"
            + "  bs.pkey\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  server_resources sr\n"
            + "  inner join business_servers bs on sr.accounting=bs.accounting and sr.server=bs.server\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=sr.server",
            connector.getConnectAs()
        );
    }

    protected Set<ServerResource> getSetBusiness() throws IOException, SQLException {
        // owns the resource
        StringBuilder sql = new StringBuilder(
            "select\n"
            + "  sr.resource,\n"
            + "  sr.server,\n"
            + "  bs.pkey\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  server_resources sr\n"
            + "  inner join business_servers bs on sr.accounting=bs.accounting and sr.server=bs.server\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  ) and (\n"
            + "    bu1.accounting=sr.accounting\n"
        );
        addOptionalInInteger(sql, "    or sr.resource in (", connector.ipAddresses.getSetBusiness(), ")\n");
        sql.append("  )");
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            sql.toString(),
            connector.getConnectAs()
        );
    }
}
