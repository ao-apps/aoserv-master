/*
 * Copyright 2010-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseBusinessAdministratorRoleService extends DatabaseAccountTypeService<Integer,BusinessAdministratorRole> implements BusinessAdministratorRoleService {

    private final ObjectFactory<BusinessAdministratorRole> objectFactory = new AutoObjectFactory<BusinessAdministratorRole>(BusinessAdministratorRole.class, connector);

    DatabaseBusinessAdministratorRoleService(DatabaseConnector connector) {
        super(connector, Integer.class, BusinessAdministratorRole.class);
    }

    @Override
    protected List<BusinessAdministratorRole> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<BusinessAdministratorRole>(),
            objectFactory,
            "select * from business_administrator_roles"
        );
    }

    /**
     * Can only see their own roles.
     */
    @Override
    protected List<BusinessAdministratorRole> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<BusinessAdministratorRole>(),
            objectFactory,
            "select\n"
            + "  *\n"
            + "from\n"
            + "  business_administrator_roles\n"
            + "where\n"
            + "  username=?",
            connector.getSwitchUser()
        );
    }

    @Override
    protected List<BusinessAdministratorRole> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<BusinessAdministratorRole>(),
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
            + "  and ba.username=bar.username",
            connector.getSwitchUser()
        );
    }
}
