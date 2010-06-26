package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServObject;
import com.aoindustries.aoserv.client.AOServerResource;
import com.aoindustries.aoserv.client.AOServerResourceService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import com.aoindustries.util.ArraySet;
import com.aoindustries.util.HashCodeComparator;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseAOServerResourceService extends DatabaseService<Integer,AOServerResource> implements AOServerResourceService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<AOServerResource> objectFactory = new AutoObjectFactory<AOServerResource>(AOServerResource.class, this);

    DatabaseAOServerResourceService(DatabaseConnector connector) {
        super(connector, Integer.class, AOServerResource.class);
    }

    @Override
    protected Set<AOServerResource> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<AOServerResource>(HashCodeComparator.getInstance()),
            objectFactory,
            "select\n"
            + "  asr.resource,\n"
            + "  asr.ao_server,\n"
            + "  bs.pkey\n"
            + "from\n"
            + "  ao_server_resources asr\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "order by\n"
            + "  asr.resource"
        );
    }

    @Override
    protected Set<AOServerResource> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<AOServerResource>(HashCodeComparator.getInstance()),
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
            + "  and ms.server=asr.ao_server\n"
            + "order by\n"
            + "  asr.resource",
            connector.getConnectAs()
        );
    }

    /**
     * Adds the extra server resources for the current user.
     */
    void addExtraAOServerResourcesBusiness(DatabaseConnection db, List<Set<? extends AOServObject<Integer,?>>> extraAoserverResources) throws SQLException {
        extraAoserverResources.add(connector.httpdServers.getSetBusiness(db));
        extraAoserverResources.add(connector.linuxGroups.getSetBusiness(db));
        extraAoserverResources.add(connector.mysqlServers.getSetBusiness(db));
        extraAoserverResources.add(connector.postgresServers.getSetBusiness(db));
    }

    @Override
    protected Set<AOServerResource> getSetBusiness(DatabaseConnection db) throws SQLException {
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
        List<Set<? extends AOServObject<Integer,?>>> extraAoserverResources = new ArrayList<Set<? extends AOServObject<Integer,?>>>();
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
        sql.append("order by\n"
                + "  resource");
        return db.executeObjectSetQuery(
            new ArraySet<AOServerResource>(HashCodeComparator.getInstance()),
            objectFactory,
            sql.toString(),
            connector.getConnectAs()
        );
    }
}
