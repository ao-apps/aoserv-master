/*
 * Copyright 2009-2011 by AO Industries, Inc.,
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
final class DatabaseServerFarmService extends DatabaseResourceService<ServerFarm> implements ServerFarmService {

    private static final String SELECT_COLUMNS =
        RESOURCE_SELECT_COLUMNS + ",\n"
        + "  sf.name,\n"
        + "  sf.description,\n"
        + "  sf.use_restricted_smtp_port"
    ;

    private final ObjectFactory<ServerFarm> objectFactory = new AutoObjectFactory<ServerFarm>(ServerFarm.class, connector);

    DatabaseServerFarmService(DatabaseConnector connector) {
        super(connector, ServerFarm.class);
    }

    @Override
    protected ArrayList<ServerFarm> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<ServerFarm>(),
            objectFactory,
            "select\n"
            + SELECT_COLUMNS + "\n"
            + "from\n"
            + "  server_farms sf\n"
            + "  inner join resources re on sf.resource=re.pkey"
        );
    }

    @Override
    protected ArrayList<ServerFarm> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<ServerFarm>(),
            objectFactory,
            "select distinct\n"
            + SELECT_COLUMNS + "\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  servers se\n"
            + "  left outer join failover_file_replications ffr on se.resource=ffr.server\n"
            + "  left outer join backup_partitions bp on ffr.backup_partition=bp.pkey\n"
            + "  left outer join servers fs on bp.ao_server=fs.resource,\n"
            + "  server_farms sf,\n"
            + "  resources re\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=se.resource\n"
            + "  and (\n"
            + "    se.farm=sf.name\n"
            + "    or fs.farm=sf.name\n"
            + "  ) and sf.resource=re.pkey",
            connector.getSwitchUser()
        );
    }

    @Override
    protected ArrayList<ServerFarm> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<ServerFarm>(),
            objectFactory,
            "select distinct\n"
            + SELECT_COLUMNS + "\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            + "  servers se,\n"
            + "  server_farms sf,\n"
            + "  resources re\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + "    (\n"
            + "      un.accounting=bs.accounting\n"
            + "      and bs.server=se.resource\n"
            + "      and se.farm=sf.name\n"
            + "    ) or un.accounting=re.accounting\n"
            + "  ) and sf.resource=re.pkey",
            connector.getSwitchUser()
        );
    }
}
