/*
 * Copyright 2009-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseAOServerResourceService extends AOServerResourceService {

    static final String SELECT_COLUMNS =
        DatabaseResourceService.SELECT_COLUMNS
        + "  asr.ao_server,\n"
        + "  bs.pkey,\n"
    ;

    /* TODO
    private final ObjectFactory<AOServerResource> objectFactory = new AutoObjectFactory<AOServerResource>(AOServerResource.class, connector);
     */
    DatabaseAOServerResourceService(DatabaseConnector connector) {
        super(connector);
    }

    /* TODO
    @Override
    protected ArrayList<AOServerResource> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<AOServerResource>(),
            objectFactory,
            "select\n"
            + DatabaseResourceService.SELECT_COLUMNS
            + "  asr.ao_server,\n"
            + "  bs.pkey\n"
            + "from\n"
            + "  ao_server_resources asr\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey"
        );
    }

    @Override
    protected ArrayList<AOServerResource> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<AOServerResource>(),
            objectFactory,
            "select\n"
            + "  asr.resource,\n"
            + "  asr.ao_server,\n"
            + "  bs.pkey\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  ao_server_resources asr\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=asr.ao_server",
            connector.getConnectAs()
        );
    }
    */
    /**
     * Adds the extra server resources for the current user.
     */
    /* TODO
    void addExtraAOServerResourcesBusiness(DatabaseConnection db, List<List<? extends AOServObject<Integer>>> extraAoserverResources) throws SQLException {
        extraAoserverResources.add(connector.httpdServers.getListBusiness(db));
        extraAoserverResources.add(connector.linuxGroups.getListBusiness(db));
        extraAoserverResources.add(connector.mysqlServers.getListBusiness(db));
        extraAoserverResources.add(connector.postgresServers.getListBusiness(db));
    }

    @Override
    protected ArrayList<AOServerResource> getListBusiness(DatabaseConnection db) throws SQLException {
        // owns the resource
        StringBuilder sql = new StringBuilder(
            "select\n"
            + "  asr.resource,\n"
            + "  asr.ao_server,\n"
            + "  bs.pkey\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  ao_server_resources asr\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  ) and bu1.accounting=asr.accounting\n"
        );
        List<List<? extends AOServObject<Integer>>> extraAoserverResources = new ArrayList<List<? extends AOServObject<Integer>>>();
        addExtraAOServerResourcesBusiness(db, extraAoserverResources);
        addOptionalInInteger(
            sql,
            "union select\n"
            + "  asr.resource,\n"
            + "  asr.ao_server,\n"
            + "  bs.pkey\n"
            + "from\n"
            + "  ao_server_resources asr\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "where\n"
            + "  asr.resource in (",
            extraAoserverResources,
            ")\n"
        );
        return db.executeObjectCollectionQuery(
            new ArrayList<AOServerResource>(),
            objectFactory,
            sql.toString(),
            connector.getConnectAs()
        );
    }
     */
}
