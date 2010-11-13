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
final class DatabaseServerResourceService extends DatabaseService<Integer,ServerResource> implements ServerResourceService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<ServerResource> objectFactory = new AutoObjectFactory<ServerResource>(ServerResource.class, this);

    DatabaseServerResourceService(DatabaseConnector connector) {
        super(connector, Integer.class, ServerResource.class);
    }

    @Override
    protected Set<ServerResource> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArraySet<ServerResource>(),
            objectFactory,
            "select\n"
            + "  sr.resource,\n"
            + "  sr.server,\n"
            + "  bs.pkey\n"
            + "from\n"
            + "  server_resources sr\n"
            + "  inner join business_servers bs on sr.accounting=bs.accounting and sr.server=bs.server\n"
            + "order by\n"
            + "  sr.resource"
        );
    }

    /**
     * Adds the extra server resources for the current user.
     */
    void addExtraServerResourcesDaemon(DatabaseConnection db, List<Set<? extends AOServObject<Integer>>> extraServerResources) throws SQLException {
        extraServerResources.add(connector.ipAddresses.getSetDaemon(db));
    }

    @Override
    protected Set<ServerResource> getSetDaemon(DatabaseConnection db) throws SQLException {
        StringBuilder sql = new StringBuilder(
            "select\n"
            + "  sr.resource,\n"
            + "  sr.server,\n"
            + "  bs.pkey\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  server_resources sr\n"
            + "  inner join business_servers bs on sr.accounting=bs.accounting and sr.server=bs.server\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=sr.server\n"
        );
        List<Set<? extends AOServObject<Integer>>> extraServerResources = new ArrayList<Set<? extends AOServObject<Integer>>>();
        addExtraServerResourcesDaemon(db, extraServerResources);
        addOptionalInInteger(
            sql,
            "union select\n"
            + "  sr.resource,\n"
            + "  sr.server,\n"
            + "  bs.pkey\n"
            + "from\n"
            + "  server_resources sr\n"
            + "  inner join business_servers bs on sr.accounting=bs.accounting and sr.server=bs.server\n"
            + "where\n"
            + "  sr.resource in (",
            extraServerResources,
            ")\n"
        );
        sql.append("order by\n"
                + "  resource");
        return db.executeObjectCollectionQuery(
            new ArraySet<ServerResource>(),
            objectFactory,
            sql.toString(),
            connector.getConnectAs()
        );
    }

    /**
     * Adds the extra server resources for the current user.
     */
    void addExtraServerResourcesBusiness(DatabaseConnection db, List<Set<? extends AOServObject<Integer>>> extraServerResources) throws SQLException {
        extraServerResources.add(connector.ipAddresses.getSetBusiness(db));
    }

    @Override
    protected Set<ServerResource> getSetBusiness(DatabaseConnection db) throws SQLException {
        // owns the resource
        StringBuilder sql = new StringBuilder(
            "select\n"
            + "  sr.resource,\n"
            + "  sr.server,\n"
            + "  bs.pkey\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  server_resources sr\n"
            + "  inner join business_servers bs on sr.accounting=bs.accounting and sr.server=bs.server\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  ) and bu1.accounting=sr.accounting\n"
        );
        List<Set<? extends AOServObject<Integer>>> extraServerResources = new ArrayList<Set<? extends AOServObject<Integer>>>();
        addExtraServerResourcesBusiness(db, extraServerResources);
        addOptionalInInteger(
            sql,
            "union select\n"
            + "  sr.resource,\n"
            + "  sr.server,\n"
            + "  bs.pkey\n"
            + "from\n"
            + "  server_resources sr\n"
            + "  inner join business_servers bs on sr.accounting=bs.accounting and sr.server=bs.server\n"
            + "where\n"
            + "  sr.resource in (",
            extraServerResources,
            ")\n"
        );
        sql.append("order by\n"
                + "  resource");
        return db.executeObjectCollectionQuery(
            new ArraySet<ServerResource>(),
            objectFactory,
            sql.toString(),
            connector.getConnectAs()
        );
    }
}
