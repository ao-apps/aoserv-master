/*
 * Copyright 2010-2011 by AO Industries, Inc.,
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
final class DatabaseFailoverFileScheduleService extends DatabaseService<Integer,FailoverFileSchedule> implements FailoverFileScheduleService {

    private final ObjectFactory<FailoverFileSchedule> objectFactory = new AutoObjectFactory<FailoverFileSchedule>(FailoverFileSchedule.class, connector);

    DatabaseFailoverFileScheduleService(DatabaseConnector connector) {
        super(connector, Integer.class, FailoverFileSchedule.class);
    }

    @Override
    protected ArrayList<FailoverFileSchedule> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<FailoverFileSchedule>(),
            objectFactory,
            "select * from failover_file_schedule"
        );
    }

    @Override
    protected ArrayList<FailoverFileSchedule> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<FailoverFileSchedule>(),
            objectFactory,
            "select\n"
            + "  ffs.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  failover_file_replications ffr,\n"
            + "  failover_file_schedule ffs\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=ffr.server\n"
            + "  and ffr.pkey=ffs.replication",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<FailoverFileSchedule> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<FailoverFileSchedule>(),
            objectFactory,
            "select\n"
            + "  ffs.*\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            + "  failover_file_replications ffr,\n"
            + "  failover_file_schedule ffs\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs.accounting\n"
            + "  and bs.server=ffr.server\n"
            + "  and ffr.pkey=ffs.replication",
            connector.getConnectAs()
        );
    }
}
