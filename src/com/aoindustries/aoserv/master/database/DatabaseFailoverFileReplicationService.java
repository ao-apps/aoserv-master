/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseFailoverFileReplicationService extends DatabaseService<Integer,FailoverFileReplication> implements FailoverFileReplicationService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<FailoverFileReplication> objectFactory = new AutoObjectFactory<FailoverFileReplication>(FailoverFileReplication.class, this);

    DatabaseFailoverFileReplicationService(DatabaseConnector connector) {
        super(connector, Integer.class, FailoverFileReplication.class);
    }

    @Override
    protected Set<FailoverFileReplication> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<FailoverFileReplication>(),
            objectFactory,
            "select * from failover_file_replications"
        );
    }

    @Override
    protected Set<FailoverFileReplication> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<FailoverFileReplication>(),
            objectFactory,
            "select\n"
            + "  ffr.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  failover_file_replications ffr\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=ffr.server",
            connector.getConnectAs()
        );
    }

    @Override
    protected Set<FailoverFileReplication> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<FailoverFileReplication>(),
            objectFactory,
            "select\n"
            + "  ffr.*\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            + "  failover_file_replications ffr\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs.accounting\n"
            + "  and bs.server=ffr.server",
            connector.getConnectAs()
        );
    }
}
