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
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseBusinessAdministratorRoleService extends DatabaseService<Integer,BusinessAdministratorRole> implements BusinessAdministratorRoleService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<BusinessAdministratorRole> objectFactory = new AutoObjectFactory<BusinessAdministratorRole>(BusinessAdministratorRole.class, this);

    DatabaseBusinessAdministratorRoleService(DatabaseConnector connector) {
        super(connector, Integer.class, BusinessAdministratorRole.class);
    }

    @Override
    protected Set<BusinessAdministratorRole> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArraySet<BusinessAdministratorRole>(),
            objectFactory,
            "select * from business_administrator_roles order by pkey"
        );
    }

    /**
     * Can only see their own roles.
     */
    @Override
    protected Set<BusinessAdministratorRole> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArraySet<BusinessAdministratorRole>(),
            objectFactory,
            "select\n"
            + "  *\n"
            + "from\n"
            + "  business_administrator_roles\n"
            + "where\n"
            + "  username=?\n"
            + "order by\n"
            + "  pkey",
            connector.getConnectAs()
        );
    }

    @Override
    protected Set<BusinessAdministratorRole> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArraySet<BusinessAdministratorRole>(),
            objectFactory,
            "select\n"
            + "  bar.*\n"
            + "from\n"
            + "  usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "  business_administrators ba,\n"
            + "  business_administrator_roles bar\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and (\n"
            + "    ba.username=un1.username\n"
            + UN1_BU1_PARENTS_OR_WHERE
            + "  )\n"
            + "  and bu1.accounting=ba.accounting\n"
            + "  and ba.username=bar.username\n"
            + "order by\n"
            + "  bar.pkey",
            connector.getConnectAs()
        );
    }
}
