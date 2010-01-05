package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.ServerFarm;
import com.aoindustries.aoserv.client.ServerFarmService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseServerFarmService extends DatabaseServiceStringKey<ServerFarm> implements ServerFarmService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<ServerFarm> objectFactory = new AutoObjectFactory<ServerFarm>(ServerFarm.class, this);

    DatabaseServerFarmService(DatabaseConnector connector) {
        super(connector, ServerFarm.class);
    }

    protected Set<ServerFarm> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from server_farms"
        );
    }

    protected Set<ServerFarm> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select distinct\n"
            + "  sf.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  servers se\n"
            + "  left outer join failover_file_replications ffr on se.pkey=ffr.server\n"
            + "  left outer join backup_partitions bp on ffr.backup_partition=bp.pkey\n"
            + "  left outer join servers fs on bp.ao_server=fs.pkey,\n"
            + "  server_farms sf\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=se.pkey\n"
            + "  and (\n"
            + "    se.farm=sf.name\n"
            + "    or fs.farm=sf.name\n"
            + "  )",
            getConnector().getConnectAs()
        );
    }

    protected Set<ServerFarm> getSetBusiness() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select distinct\n"
            + "  sf.*\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            + "  servers se,\n"
            + "  server_farms sf\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + "    (\n"
            + "      un.accounting=bs.accounting\n"
            + "      and bs.server=se.pkey\n"
            + "      and se.farm=sf.name\n"
            + "    ) or un.accounting=sf.owner\n"
            + "  )",
            getConnector().getConnectAs()
        );
    }
}