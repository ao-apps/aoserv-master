/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseServerResourceService extends ServerResourceService {

    static final String SELECT_COLUMNS =
        DatabaseResourceService.SELECT_COLUMNS
        + "  sr.server,\n"
        + "  bs.pkey,\n"
    ;

    /* TODO
    private final ObjectFactory<ServerResource> objectFactory = new AutoObjectFactory<ServerResource>(ServerResource.class, connector);
     */
    DatabaseServerResourceService(DatabaseConnector connector) {
        super(connector);
    }
    /* TODO
    @Override
    protected ArrayList<ServerResource> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<ServerResource>(),
            objectFactory,
            "select\n"
            + "  sr.resource,\n"
            + "  sr.server,\n"
            + "  bs.pkey\n"
            + "from\n"
            + "  server_resources sr\n"
            + "  inner join business_servers bs on sr.accounting=bs.accounting and sr.server=bs.server"
        );
    }
    */
    /**
     * Adds the extra server resources for the current user.
     */
    /* TODO
    void addExtraServerResourcesDaemon(DatabaseConnection db, List<List<? extends AOServObject<Integer>>> extraServerResources) throws SQLException {
        extraServerResources.add(connector.ipAddresses.getListDaemon(db));
    }

    @Override
    protected ArrayList<ServerResource> getListDaemon(DatabaseConnection db) throws SQLException {
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
        List<List<? extends AOServObject<Integer>>> extraServerResources = new ArrayList<List<? extends AOServObject<Integer>>>();
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
        return db.executeObjectCollectionQuery(
            new ArrayList<ServerResource>(),
            objectFactory,
            sql.toString(),
            connector.getConnectAs()
        );
    }
    */
    /**
     * Adds the extra server resources for the current user.
     */
    /* TODO
    void addExtraServerResourcesBusiness(DatabaseConnection db, List<List<? extends AOServObject<Integer>>> extraServerResources) throws SQLException {
        extraServerResources.add(connector.ipAddresses.getListBusiness(db));
    }

    @Override
    protected ArrayList<ServerResource> getListBusiness(DatabaseConnection db) throws SQLException {
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
        List<List<? extends AOServObject<Integer>>> extraServerResources = new ArrayList<List<? extends AOServObject<Integer>>>();
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
        return db.executeObjectCollectionQuery(
            new ArrayList<ServerResource>(),
            objectFactory,
            sql.toString(),
            connector.getConnectAs()
        );
    }
     */
}
