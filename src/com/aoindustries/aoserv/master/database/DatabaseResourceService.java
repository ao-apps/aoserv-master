package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.Resource;
import com.aoindustries.aoserv.client.ResourceService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseResourceService extends DatabaseServiceIntegerKey<Resource> implements ResourceService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<Resource> objectFactory = new AutoObjectFactory<Resource>(Resource.class, this);

    DatabaseResourceService(DatabaseConnector connector) {
        super(connector, Resource.class);
    }

    protected Set<Resource> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from resources"
        );
    }

    protected Set<Resource> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  re.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  ao_server_resources asr,\n"
            + "  resources re\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=asr.ao_server\n"
            + "  and asr.resource=re.pkey",
            getConnector().getConnectAs()
        );
    }

    protected Set<Resource> getSetBusiness() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            // owns the resource
            "select\n"
            + "  re.*\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  resources re\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=re.accounting\n"
            // has access to the mysql_servers
            + "union select\n"
            + "  re.*\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            + "  mysql_servers ms,\n"
            + "  resources re\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs.accounting\n"
            + "  and bs.server=ms.ao_server\n"
            + "  and ms.ao_server_resource=re.pkey\n",
            getConnector().getConnectAs(),
            getConnector().getConnectAs()
        );
    }
}
