package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.BusinessServer;
import com.aoindustries.aoserv.client.BusinessServerService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseBusinessServerService extends DatabaseServiceIntegerKey<BusinessServer> implements BusinessServerService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<BusinessServer> objectFactory = new AutoObjectFactory<BusinessServer>(BusinessServer.class, this);

    DatabaseBusinessServerService(DatabaseConnector connector) {
        super(connector, BusinessServer.class);
    }

    protected Set<BusinessServer> getSetMaster(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from business_servers"
        );
    }

    protected Set<BusinessServer> getSetDaemon(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select distinct\n"
            + "  bs.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  business_servers bs\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=bs.server",
            connector.getConnectAs()
        );
    }

    protected Set<BusinessServer> getSetBusiness(DatabaseConnection db) throws IOException, SQLException {
        // owns the resource
        StringBuilder sql = new StringBuilder(
            "select\n"
            + "  bs.*\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  business_servers bs\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  ) and (\n"
            + "    bu1.accounting=bs.accounting\n"
        );
        addOptionalInInteger(sql, "    or (bs.accounting, bs.server) in (select accounting, server from resources where pkey in (", connector.ipAddresses.getSetBusiness(db), "))\n");
        addOptionalInInteger(sql, "    or (bs.accounting, bs.server) in (select accounting, server from resources where pkey in (", connector.linuxGroups.getSetBusiness(db), "))\n");
        addOptionalInInteger(sql, "    or (bs.accounting, bs.server) in (select accounting, server from resources where pkey in (", connector.mysqlServers.getSetBusiness(db), "))\n");
        addOptionalInInteger(sql, "    or (bs.accounting, bs.server) in (select accounting, server from resources where pkey in (", connector.postgresServers.getSetBusiness(db), "))\n");
        sql.append("  )");
        return db.executeObjectSetQuery(
            objectFactory,
            sql.toString(),
            connector.getConnectAs()
        );
    }
}
