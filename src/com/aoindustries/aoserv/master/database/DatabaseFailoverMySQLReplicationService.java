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
final class DatabaseFailoverMySQLReplicationService extends DatabaseService<Integer,FailoverMySQLReplication> implements FailoverMySQLReplicationService {

    private final ObjectFactory<FailoverMySQLReplication> objectFactory = new AutoObjectFactory<FailoverMySQLReplication>(FailoverMySQLReplication.class, connector);

    DatabaseFailoverMySQLReplicationService(DatabaseConnector connector) {
        super(connector, Integer.class, FailoverMySQLReplication.class);
    }

    @Override
    protected ArrayList<FailoverMySQLReplication> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<FailoverMySQLReplication>(),
            objectFactory,
            "select * from failover_mysql_replications"
        );
    }

    @Override
    protected ArrayList<FailoverMySQLReplication> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<FailoverMySQLReplication>(),
            objectFactory,
            "select\n"
            + "  fmr.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  failover_file_replications ffr,\n"
            + "  failover_mysql_replications fmr\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and (\n"
            + "    (\n"
            // ao_server-based
            + "      ms.server=fmr.ao_server\n"
            + "    ) or (\n"
            // replication-based
            + "      ms.server=ffr.server\n"
            + "      and ffr.pkey=fmr.replication\n"
            + "    )\n"
            + "  )",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<FailoverMySQLReplication> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<FailoverMySQLReplication>(),
            objectFactory,
            "select distinct\n"
            + "  fmr.*\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            + "  failover_file_replications ffr,\n"
            + "  failover_mysql_replications fmr\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs.accounting\n"
            + "  and (\n"
            + "    (\n"
            // ao_server-based
            + "      bs.server=fmr.ao_server\n"
            + "    ) or (\n"
            // replication-based
            + "      bs.server=ffr.server\n"
            + "      and ffr.pkey=fmr.replication\n"
            + "    )\n"
            + "  )",
            connector.getConnectAs()
        );
    }
}
