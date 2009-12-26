package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServerResource;
import com.aoindustries.aoserv.client.AOServerResourceService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseAOServerResourceService extends DatabaseServiceIntegerKey<AOServerResource> implements AOServerResourceService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<AOServerResource> objectFactory = new AutoObjectFactory<AOServerResource>(AOServerResource.class, this);

    DatabaseAOServerResourceService(DatabaseConnector connector) {
        super(connector, AOServerResource.class);
    }

    protected Set<AOServerResource> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select resource, ao_server from ao_server_resources"
        );
    }

    protected Set<AOServerResource> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  asr.resource, asr.ao_server\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  ao_server_resources asr\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=asr.ao_server",
            connector.getConnectAs()
        );
    }

    protected Set<AOServerResource> getSetBusiness() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            // owns the resource
            "select\n"
            + "  re.resource,\n"
            + "  re.ao_server\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  ao_server_resources re\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=re.accounting\n"
            // has access to the mysql_servers
            + "union select\n"
            + "  ms.ao_server_resource,\n"
            + "  ms.ao_server\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            + "  mysql_servers ms\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs.accounting\n"
            + "  and bs.server=ms.ao_server",
            connector.getConnectAs(),
            connector.getConnectAs()
        );
    }
}
