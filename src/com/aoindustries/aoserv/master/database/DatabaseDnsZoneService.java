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
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseDnsZoneService extends DatabaseService<Integer,DnsZone> implements DnsZoneService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<DnsZone> objectFactory = new AutoObjectFactory<DnsZone>(DnsZone.class, this);

    DatabaseDnsZoneService(DatabaseConnector connector) {
        super(connector, Integer.class, DnsZone.class);
    }

    @Override
    protected Set<DnsZone> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArraySet<DnsZone>(),
            objectFactory,
            "select\n"
            + "  resource,\n"
            + "  zone,\n"
            + "  file,\n"
            + "  hostmaster,\n"
            + "  serial,\n"
            + "  ttl\n"
            + "from\n"
            + "  dns_zones\n"
            + "order by\n"
            + "  resource"
        );
    }

    @Override
    protected Set<DnsZone> getSetDaemon(DatabaseConnection db) throws RemoteException, SQLException {
        MasterUser mu = connector.factory.rootConnector.getBusinessAdministrators().get(connector.getConnectAs()).getMasterUser();
        if(mu!=null && mu.isActive() && mu.isDnsAdmin()) {
            return db.executeObjectCollectionQuery(
                new ArraySet<DnsZone>(),
                objectFactory,
                "select\n"
                + "  resource,\n"
                + "  zone,\n"
                + "  file,\n"
                + "  hostmaster,\n"
                + "  serial,\n"
                + "  ttl\n"
                + "from\n"
                + "  dns_zones\n"
                + "order by\n"
                + "  resource"
            );
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    protected Set<DnsZone> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArraySet<DnsZone>(),
            objectFactory,
            "select\n"
            + "  dz.resource,\n"
            + "  dz.zone,\n"
            + "  dz.file,\n"
            + "  dz.hostmaster,\n"
            + "  dz.serial,\n"
            + "  dz.ttl\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  dns_zones dz\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=dz.accounting\n"
            + "order by\n"
            + "  dz.resource",
            connector.getConnectAs()
        );
    }
}
