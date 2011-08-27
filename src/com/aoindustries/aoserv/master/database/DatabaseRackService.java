/*
 * Copyright 2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseRackService extends DatabaseResourceService<Rack> implements RackService {

    private static final String SELECT_COLUMNS =
        RESOURCE_SELECT_COLUMNS + ",\n"
        + "  ra.farm,\n"
        + "  ra.name,\n"
        + "  ra.max_power,\n"
        + "  ra.total_rack_units"
    ;
    private final ObjectFactory<Rack> objectFactory = new AutoObjectFactory<Rack>(Rack.class, connector);

    DatabaseRackService(DatabaseConnector connector) {
        super(connector, Rack.class);
    }

    @Override
    protected List<Rack> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Rack>(),
            objectFactory,
            "select\n"
            + SELECT_COLUMNS + "\n"
            + "from\n"
            + "  racks ra\n"
            + "  inner join resources re on ra.resource=re.pkey"
        );
    }

    @Override
    protected List<Rack> getListDaemon(DatabaseConnection db) throws RemoteException, SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Rack>(),
            objectFactory,
            "select distinct\n"
            + SELECT_COLUMNS + "\n"
            + "from\n"
            + "  master_servers ms\n"
            + "  inner join physical_servers ps on ms.server=ps.resource\n"
            + "  inner join racks ra on ps.rack=ra.resource\n"
            + "  inner join resources re on ra.resource=re.pkey\n"
            + "where\n"
            + "  ms.username=?",
            connector.getSwitchUser()
        );
    }

    @Override
    protected List<Rack> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Rack>(),
            objectFactory,
            "select distinct\n"
            + SELECT_COLUMNS + "\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            // Allow servers it replicates to
            //+ "  left join failover_file_replications ffr on bs.server=ffr.server\n"
            //+ "  left join backup_partitions bp on ffr.backup_partition=bp.pkey,\n"
            + "  physical_servers ps,\n"
            + "  racks ra,\n"
            + "  resources re\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs.accounting\n"
            + "  and (\n"
            + "    bs.server=ps.resource\n"
            // Allow servers it replicates to
            //+ "    or bp.ao_server=ps.resource\n"
            + "  ) and ps.rack=ra.resource\n"
            + " and ra.resource=re.pkey",
            connector.getSwitchUser()
        );
    }
}
