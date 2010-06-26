/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.DisableLog;
import com.aoindustries.aoserv.client.DisableLogService;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.aoserv.client.validator.ValidationException;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import com.aoindustries.util.ArraySet;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseDisableLogService extends DatabaseService<Integer,DisableLog> implements DisableLogService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<DisableLog> objectFactory = new ObjectFactory<DisableLog>() {
        @Override
        public DisableLog createObject(ResultSet result) throws SQLException {
            try {
                return new DisableLog(
                    DatabaseDisableLogService.this,
                    result.getInt("pkey"),
                    result.getLong("time"),
                    AccountingCode.valueOf(result.getString("accounting")),
                    UserId.valueOf(result.getString("disabled_by")),
                    result.getString("disable_reason")
                );
            } catch(ValidationException err) {
                throw new SQLException(err);
            }
        }
    };

    DatabaseDisableLogService(DatabaseConnector connector) {
        super(connector, Integer.class, DisableLog.class);
    }

    @Override
    protected Set<DisableLog> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<DisableLog>(),
            objectFactory,
            "select\n"
            + "  pkey,\n"
            + "  (extract(epoch from time)*1000)::int8 as time,\n"
            + "  accounting,\n"
            + "  disabled_by,\n"
            + "  disable_reason\n"
            + "from\n"
            + "  disable_log\n"
            + "order by\n"
            + "  pkey"
        );
    }

    @Override
    protected Set<DisableLog> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<DisableLog>(),
            objectFactory,
            "select distinct\n"
            + "  dl.pkey,\n"
            + "  (extract(epoch from dl.time)*1000)::int8 as time,\n"
            + "  dl.accounting,\n"
            + "  dl.disabled_by,\n"
            + "  dl.disable_reason\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  ao_servers ao\n"
            + "  left join ao_servers ff on ao.server=ff.failover_server,\n"
            + "  business_servers bs,\n"
            + "  disable_log dl\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=ao.server\n"
            + "  and (\n"
            + "    ao.server=bs.server\n"
            + "    or ff.server=bs.server\n"
            + "  ) and bs.accounting=dl.accounting\n"
            + "order by\n"
            + "  dl.pkey",
            connector.getConnectAs()
        );
    }

    @Override
    protected Set<DisableLog> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<DisableLog>(),
            objectFactory,
            "select\n"
            + "  dl.pkey,\n"
            + "  (extract(epoch from dl.time)*1000)::int8 as time,\n"
            + "  dl.accounting,\n"
            + "  dl.disabled_by,\n"
            + "  dl.disable_reason\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  disable_log dl\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=dl.accounting\n"
            + "order by\n"
            + "  dl.pkey",
            connector.getConnectAs()
        );
    }
}
