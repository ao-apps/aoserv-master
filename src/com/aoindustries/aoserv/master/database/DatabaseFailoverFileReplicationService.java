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
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseFailoverFileReplicationService extends DatabaseAccountTypeService<Integer,FailoverFileReplication> implements FailoverFileReplicationService {

    private final ObjectFactory<FailoverFileReplication> objectFactory = new AutoObjectFactory<FailoverFileReplication>(FailoverFileReplication.class, connector);

    DatabaseFailoverFileReplicationService(DatabaseConnector connector) {
        super(connector, Integer.class, FailoverFileReplication.class);
    }

    @Override
    protected List<FailoverFileReplication> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<FailoverFileReplication>(),
            objectFactory,
            "select * from failover_file_replications"
        );
    }

    @Override
    protected List<FailoverFileReplication> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<FailoverFileReplication>(),
            objectFactory,
            "select\n"
            + "  ffr.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  failover_file_replications ffr\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=ffr.server",
            connector.getSwitchUser()
        );
    }

    @Override
    protected List<FailoverFileReplication> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<FailoverFileReplication>(),
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
            connector.getSwitchUser()
        );
    }
}
