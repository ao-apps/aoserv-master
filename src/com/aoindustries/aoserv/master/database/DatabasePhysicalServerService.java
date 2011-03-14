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
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabasePhysicalServerService extends DatabaseServerService<PhysicalServer> implements PhysicalServerService {

    private static final String SELECT_COLUMNS =
        SERVER_SELECT_COLUMNS + ",\n"
        + "  ps.rack,\n"
        + "  ps.rack_units,\n"
        + "  ps.ram,\n"
        + "  ps.processor_type,\n"
        + "  ps.processor_speed,\n"
        + "  ps.processor_cores,\n"
        + "  ps.max_power,\n"
        + "  ps.supports_hvm"
    ;

    private final ObjectFactory<PhysicalServer> objectFactory = new AutoObjectFactory<PhysicalServer>(PhysicalServer.class, connector);

    DatabasePhysicalServerService(DatabaseConnector connector) {
        super(connector, PhysicalServer.class);
    }

    @Override
    protected ArrayList<PhysicalServer> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<PhysicalServer>(),
            objectFactory,
            "select\n"
            + SELECT_COLUMNS + "\n"
            + "from\n"
            + "  physical_servers ps\n"
            + "  inner join servers se on ps.resource=se.resource\n"
            + "  inner join resources re on se.resource=re.pkey"
        );
    }

    @Override
    protected ArrayList<PhysicalServer> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<PhysicalServer>(),
            objectFactory,
            "select distinct\n"
            + SELECT_COLUMNS + "\n"
            + "from\n"
            + "  master_servers ms\n"
            + "  left join ao_servers ao on ms.server=ao.server\n"
            // Allow its failover parent
            + "  left join ao_servers ff on ao.failover_server=ff.server\n"
            // Allow its failover children
            + "  left join ao_servers fs on ao.server=fs.failover_server\n"
            // Allow servers it replicates to
            + "  left join failover_file_replications ffr on ms.server=ffr.server\n"
            + "  left join backup_partitions bp on ffr.backup_partition=bp.pkey,\n"
            + "  servers se,\n"
            + "  physical_servers ps,\n"
            + "  resources re\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and (\n"
            // Allow direct access
            + "    ms.server=se.resource\n"
            // Allow its failover parent
            + "    or ff.server=se.resource\n"
            // Allow its failover children
            + "    or fs.server=se.resource\n"
            // Allow servers it replicates to
            + "    or bp.ao_server=se.resource\n"
            + "  ) and se.resource=ps.resource\n"
            + "  and ps.resource=re.pkey",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<PhysicalServer> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<PhysicalServer>(),
            objectFactory,
            "select distinct\n"
            + SELECT_COLUMNS + "\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            // Allow servers it replicates to
            //+ "  left join failover_file_replications ffr on bs.server=ffr.server\n"
            //+ "  left join backup_partitions bp on ffr.backup_partition=bp.pkey,\n"
            + "  servers se,\n"
            + "  physical_servers ps,\n"
            + "  resources re\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs.accounting\n"
            + "  and (\n"
            + "    bs.server=se.resource\n"
            // Allow servers it replicates to
            //+ "    or bp.ao_server=se.resource\n"
            + "  ) and se.resource=ps.resource\n"
            + "  and ps.resource=re.pkey",
            connector.getConnectAs()
        );
    }
}
