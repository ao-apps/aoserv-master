/*
 * Copyright 2009-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.client.validator.*;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseDisableLogService extends DatabaseAccountTypeService<Integer,DisableLog> implements DisableLogService {

    private final ObjectFactory<DisableLog> objectFactory = new ObjectFactory<DisableLog>() {
        @Override
        public DisableLog createObject(ResultSet result) throws SQLException {
            try {
                return new DisableLog(
                    connector,
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
    protected ArrayList<DisableLog> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<DisableLog>(),
            objectFactory,
            "select\n"
            + "  pkey,\n"
            + "  (extract(epoch from time)*1000)::int8 as time,\n"
            + "  accounting,\n"
            + "  disabled_by,\n"
            + "  disable_reason\n"
            + "from\n"
            + "  disable_log"
        );
    }

    @Override
    protected ArrayList<DisableLog> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<DisableLog>(),
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
            + "  ) and bs.accounting=dl.accounting",
            connector.getSwitchUser()
        );
    }

    @Override
    protected ArrayList<DisableLog> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<DisableLog>(),
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
            + "  and bu1.accounting=dl.accounting",
            connector.getSwitchUser()
        );
    }
}
