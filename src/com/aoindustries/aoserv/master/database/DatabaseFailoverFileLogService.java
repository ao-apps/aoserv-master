/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import com.aoindustries.util.ArraySet;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseFailoverFileLogService extends DatabaseService<Integer,FailoverFileLog> implements FailoverFileLogService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<FailoverFileLog> objectFactory = new ObjectFactory<FailoverFileLog>() {
        @Override
        public FailoverFileLog createObject(ResultSet result) throws SQLException {
            return new FailoverFileLog(
                DatabaseFailoverFileLogService.this,
                result.getInt("pkey"),
                result.getInt("replication"),
                result.getLong("start_time"),
                result.getLong("end_time"),
                result.getInt("scanned"),
                result.getInt("updated"),
                result.getLong("bytes"),
                result.getBoolean("is_successful")
            );
        }
    };

    DatabaseFailoverFileLogService(DatabaseConnector connector) {
        super(connector, Integer.class, FailoverFileLog.class);
    }

    @Override
    protected Set<FailoverFileLog> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArraySet<FailoverFileLog>(),
            objectFactory,
            "select\n"
            + "  pkey,\n"
            + "  replication,\n"
            + "  (extract(epoch from start_time)*1000)::int8 as start_time,\n"
            + "  (extract(epoch from end_time)*1000)::int8 as end_time,\n"
            + "  scanned,\n"
            + "  updated,\n"
            + "  bytes,\n"
            + "  is_successful\n"
            + "from\n"
            + "  failover_file_log\n"
            + "order by\n"
            + "  pkey"
        );
    }

    @Override
    protected Set<FailoverFileLog> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArraySet<FailoverFileLog>(),
            objectFactory,
            "select\n"
            + "  ffl.pkey,\n"
            + "  ffl.replication,\n"
            + "  (extract(epoch from ffl.start_time)*1000)::int8 as start_time,\n"
            + "  (extract(epoch from ffl.end_time)*1000)::int8 as end_time,\n"
            + "  ffl.scanned,\n"
            + "  ffl.updated,\n"
            + "  ffl.bytes,\n"
            + "  ffl.is_successful\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  failover_file_replications ffr,\n"
            + "  failover_file_log ffl\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=ffr.server\n"
            + "  and ffr.pkey=ffl.replication\n"
            + "order by\n"
            + "  pkey",
            connector.getConnectAs()
        );
    }

    @Override
    protected Set<FailoverFileLog> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArraySet<FailoverFileLog>(),
            objectFactory,
            "select\n"
            + "  ffl.pkey,\n"
            + "  ffl.replication,\n"
            + "  (extract(epoch from ffl.start_time)*1000)::int8 as start_time,\n"
            + "  (extract(epoch from ffl.end_time)*1000)::int8 as end_time,\n"
            + "  ffl.scanned,\n"
            + "  ffl.updated,\n"
            + "  ffl.bytes,\n"
            + "  ffl.is_successful\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            + "  failover_file_replications ffr,\n"
            + "  failover_file_log ffl\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs.accounting\n"
            + "  and bs.server=ffr.server\n"
            + "  and ffr.pkey=ffl.replication\n"
            + "order by\n"
            + "  pkey",
            connector.getConnectAs()
        );
    }
}
