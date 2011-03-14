/*
 * Copyright 2009-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;

/**
 * @author  AO Industries, Inc.
 */
abstract class DatabaseServerService<V extends Server> extends DatabaseResourceService<V> {

    static final String SERVER_SELECT_COLUMNS =
        RESOURCE_SELECT_COLUMNS + ",\n"
        + "  se.farm,\n"
        + "  se.description,\n"
        + "  se.operating_system_version,\n"
        + "  se.name,\n"
        + "  se.monitoring_enabled"
    ;

    //private final ObjectFactory<Server> objectFactory = new AutoObjectFactory<Server>(Server.class, connector);

    DatabaseServerService(DatabaseConnector connector, Class<V> valueClass) {
        super(connector, valueClass);
    }

    /*
    @Override
    protected ArrayList<Server> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Server>(),
            objectFactory,
            "select * from servers"
        );
    }

    @Override
    protected ArrayList<Server> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Server>(),
            objectFactory,
            "select distinct\n"
            + "  se.*\n"
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
            + "  servers se\n"
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
            + "  )",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<Server> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Server>(),
            objectFactory,
            "select distinct\n"
            + "  se.*\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            // Allow servers it replicates to
            //+ "  left join failover_file_replications ffr on bs.server=ffr.server\n"
            //+ "  left join backup_partitions bp on ffr.backup_partition=bp.pkey,\n"
            + "  servers se\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs.accounting\n"
            + "  and (\n"
            + "    bs.server=se.resource\n"
            // Allow servers it replicates to
            //+ "    or bp.ao_server=se.resource\n"
            + "  )",
            connector.getConnectAs()
        );
    }
     */
}
