package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.FailoverFileLog;
import com.aoindustries.aoserv.client.FailoverFileLogService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseFailoverFileLogService extends DatabaseService<Integer,FailoverFileLog> implements FailoverFileLogService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<FailoverFileLog> objectFactory = new AutoObjectFactory<FailoverFileLog>(FailoverFileLog.class, this);

    DatabaseFailoverFileLogService(DatabaseConnector connector) {
        super(connector, Integer.class, FailoverFileLog.class);
    }

    protected Set<FailoverFileLog> getSetMaster(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from failover_file_log"
        );
    }

    protected Set<FailoverFileLog> getSetDaemon(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  ffl.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  failover_file_replications ffr,\n"
            + "  failover_file_log ffl\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=ffr.server\n"
            + "  and ffr.pkey=ffl.replication",
            connector.getConnectAs()
        );
    }

    protected Set<FailoverFileLog> getSetBusiness(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  ffl.*\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            + "  failover_file_replications ffr,\n"
            + "  failover_file_log ffl\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs.accounting\n"
            + "  and bs.server=ffr.server\n"
            + "  and ffr.pkey=ffl.replication",
            connector.getConnectAs()
        );
    }
}
