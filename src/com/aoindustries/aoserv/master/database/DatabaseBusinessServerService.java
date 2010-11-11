/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import com.aoindustries.util.ArraySet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseBusinessServerService extends DatabaseService<Integer,BusinessServer> implements BusinessServerService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<BusinessServer> objectFactory = new AutoObjectFactory<BusinessServer>(BusinessServer.class, this);

    DatabaseBusinessServerService(DatabaseConnector connector) {
        super(connector, Integer.class, BusinessServer.class);
    }

    @Override
    protected Set<BusinessServer> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArraySet<BusinessServer>(),
            objectFactory,
            "select * from business_servers order by pkey"
        );
    }

    @Override
    protected Set<BusinessServer> getSetDaemon(DatabaseConnection db) throws SQLException {
        StringBuilder sql = new StringBuilder(
            "select distinct\n"
            + "  bs.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  business_servers bs\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=bs.server\n"
        );
        // Extra net_binds
        addOptionalInInteger(
            sql,
            "union select\n"
            + "  bs.*\n"
            + "from\n"
            + "  net_binds nb\n"
            + "  inner join business_servers bs on nb.business_server=bs.pkey\n"
            + "where\n"
            + "  nb.pkey in (",
            connector.netBinds.getSetDaemon(db),
            ")\n"
        );
        // Extra server_resources
        List<Set<? extends AOServObject<Integer,?>>> extraServerResources = new ArrayList<Set<? extends AOServObject<Integer,?>>>();
        connector.serverResources.addExtraServerResourcesDaemon(db, extraServerResources);
        addOptionalInInteger(
            sql,
            "union select\n"
            + "  bs.*\n"
            + "from\n"
            + "  server_resources sr\n"
            + "  inner join business_servers bs on sr.accounting=bs.accounting and sr.server=bs.server\n"
            + "where\n"
            + "  sr.resource in (",
            extraServerResources,
            ")\n"
        );
        sql.append("order by\n"
                + "  pkey");
        return db.executeObjectCollectionQuery(
            new ArraySet<BusinessServer>(),
            objectFactory,
            sql.toString(),
            connector.getConnectAs()
        );
    }

    @Override
    protected Set<BusinessServer> getSetBusiness(DatabaseConnection db) throws SQLException {
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
            + "  ) and bu1.accounting=bs.accounting\n"
        );
        // Extra net_binds
        addOptionalInInteger(
            sql,
            "union select\n"
            + "  bs.*\n"
            + "from\n"
            + "  net_binds nb\n"
            + "  inner join business_servers bs on nb.business_server=bs.pkey\n"
            + "where\n"
            + "  nb.pkey in (",
            connector.netBinds.getSetBusiness(db),
            ")\n"
        );
        // Extra server_resources
        List<Set<? extends AOServObject<Integer,?>>> extraServerResources = new ArrayList<Set<? extends AOServObject<Integer,?>>>();
        connector.serverResources.addExtraServerResourcesBusiness(db, extraServerResources);
        addOptionalInInteger(
            sql,
            "union select\n"
            + "  bs.*\n"
            + "from\n"
            + "  server_resources sr\n"
            + "  inner join business_servers bs on sr.accounting=bs.accounting and sr.server=bs.server\n"
            + "where\n"
            + "  sr.resource in (",
            extraServerResources,
            ")\n"
        );
        // Extra ao_server_resources
        List<Set<? extends AOServObject<Integer,?>>> extraAoserverResources = new ArrayList<Set<? extends AOServObject<Integer,?>>>();
        connector.aoserverResources.addExtraAOServerResourcesBusiness(db, extraAoserverResources);
        addOptionalInInteger(
            sql,
            "union select\n"
            + "  bs.*\n"
            + "from\n"
            + "  ao_server_resources asr\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "where\n"
            + "  asr.resource in (",
            extraAoserverResources,
            ")\n"
        );
        sql.append("order by\n"
                + "  pkey");
        return db.executeObjectCollectionQuery(
            new ArraySet<BusinessServer>(),
            objectFactory,
            sql.toString(),
            connector.getConnectAs()
        );
    }
}
