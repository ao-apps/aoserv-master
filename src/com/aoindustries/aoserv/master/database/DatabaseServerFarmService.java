/*
 * Copyright 2009-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.client.validator.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseServerFarmService extends DatabaseService<DomainLabel,ServerFarm> implements ServerFarmService {

    private final ObjectFactory<ServerFarm> objectFactory = new AutoObjectFactory<ServerFarm>(ServerFarm.class, connector);

    DatabaseServerFarmService(DatabaseConnector connector) {
        super(connector, DomainLabel.class, ServerFarm.class);
    }

    @Override
    protected ArrayList<ServerFarm> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<ServerFarm>(),
            objectFactory,
            "select * from server_farms"
        );
    }

    @Override
    protected ArrayList<ServerFarm> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<ServerFarm>(),
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
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<ServerFarm> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<ServerFarm>(),
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
            connector.getConnectAs()
        );
    }
}
