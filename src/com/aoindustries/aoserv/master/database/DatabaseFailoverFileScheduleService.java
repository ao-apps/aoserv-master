package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.FailoverFileSchedule;
import com.aoindustries.aoserv.client.FailoverFileScheduleService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseFailoverFileScheduleService extends DatabaseServiceIntegerKey<FailoverFileSchedule> implements FailoverFileScheduleService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<FailoverFileSchedule> objectFactory = new AutoObjectFactory<FailoverFileSchedule>(FailoverFileSchedule.class, this);

    DatabaseFailoverFileScheduleService(DatabaseConnector connector) {
        super(connector, FailoverFileSchedule.class);
    }

    protected Set<FailoverFileSchedule> getSetMaster(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from failover_file_schedule"
        );
    }

    protected Set<FailoverFileSchedule> getSetDaemon(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
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

    protected Set<FailoverFileSchedule> getSetBusiness(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
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
