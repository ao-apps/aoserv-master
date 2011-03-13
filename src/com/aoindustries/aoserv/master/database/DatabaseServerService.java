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
final class DatabaseServerService extends DatabaseService<Integer,Server> implements ServerService {

    private final ObjectFactory<Server> objectFactory = new AutoObjectFactory<Server>(Server.class, connector);

    DatabaseServerService(DatabaseConnector connector) {
        super(connector, Integer.class, Server.class);
    }

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
            + "    ms.server=se.pkey\n"
            // Allow its failover parent
            + "    or ff.server=se.pkey\n"
            // Allow its failover children
            + "    or fs.server=se.pkey\n"
            // Allow servers it replicates to
            + "    or bp.ao_server=se.pkey\n"
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
            + "    bs.server=se.pkey\n"
            // Allow servers it replicates to
            //+ "    or bp.ao_server=se.pkey\n"
            + "  )",
            connector.getConnectAs()
        );
    }
}
