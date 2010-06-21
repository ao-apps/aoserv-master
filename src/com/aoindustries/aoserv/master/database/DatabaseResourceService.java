/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.Resource;
import com.aoindustries.aoserv.client.ResourceService;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.aoserv.client.validator.ValidationException;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseResourceService extends DatabaseService<Integer,Resource> implements ResourceService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<Resource> objectFactory = new ObjectFactory<Resource>() {
        @Override
        public Resource createObject(ResultSet result) throws SQLException {
            try {
                return new Resource(
                    DatabaseResourceService.this,
                    result.getInt("pkey"),
                    result.getString("resource_type"),
                    AccountingCode.valueOf(result.getString("accounting")),
                    result.getTimestamp("created"),
                    UserId.valueOf(result.getString("created_by")),
                    (Integer)result.getObject("disable_log"),
                    result.getTimestamp("last_enabled")
                );
            } catch(ValidationException err) {
                throw new SQLException(err);
            }
        }
    };

    DatabaseResourceService(DatabaseConnector connector) {
        super(connector, Integer.class, Resource.class);
    }

    @Override
    protected Set<Resource> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from resources"
        );
    }

    @Override
    protected Set<Resource> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            // ao_server_resources
            "select\n"
            + "  re.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  ao_server_resources asr,\n"
            + "  resources re\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=asr.ao_server\n"
            + "  and asr.resource=re.pkey\n"
            // server_resources
            + "union select\n"
            + "  re.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  server_resources sr,\n"
            + "  resources re\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=sr.server\n"
            + "  and sr.resource=re.pkey",
            connector.getConnectAs(),
            connector.getConnectAs()
        );
    }

    @Override
    protected Set<Resource> getSetBusiness(DatabaseConnection db) throws SQLException {
        // owns the resource
        StringBuilder sql = new StringBuilder(
            "select\n"
            + "  re.*\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  resources re\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and (\n"
            + "    bu1.accounting=re.accounting\n"
        );
        addOptionalInInteger(sql, "    or re.pkey in (", connector.httpdServers.getSetBusiness(db), ")\n");
        addOptionalInInteger(sql, "    or re.pkey in (", connector.ipAddresses.getSetBusiness(db), ")\n");
        addOptionalInInteger(sql, "    or re.pkey in (", connector.linuxGroups.getSetBusiness(db), ")\n");
        addOptionalInInteger(sql, "    or re.pkey in (", connector.mysqlServers.getSetBusiness(db), ")\n");
        addOptionalInInteger(sql, "    or re.pkey in (", connector.postgresServers.getSetBusiness(db), ")\n");
        sql.append("  )");
        return db.executeObjectSetQuery(
            objectFactory,
            sql.toString(),
            connector.getConnectAs()
        );
    }
}
