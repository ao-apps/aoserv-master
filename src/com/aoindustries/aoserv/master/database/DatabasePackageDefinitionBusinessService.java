/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.PackageDefinitionBusiness;
import com.aoindustries.aoserv.client.PackageDefinitionBusinessService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import com.aoindustries.util.ArraySet;
import com.aoindustries.util.HashCodeComparator;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabasePackageDefinitionBusinessService extends DatabaseService<Integer,PackageDefinitionBusiness> implements PackageDefinitionBusinessService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<PackageDefinitionBusiness> objectFactory = new AutoObjectFactory<PackageDefinitionBusiness>(PackageDefinitionBusiness.class, this);

    DatabasePackageDefinitionBusinessService(DatabaseConnector connector) {
        super(connector, Integer.class, PackageDefinitionBusiness.class);
    }

    @Override
    protected Set<PackageDefinitionBusiness> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<PackageDefinitionBusiness>(HashCodeComparator.getInstance()),
            objectFactory,
            "select pkey, package_definition, accounting, display, description, approved from package_definition_businesses order by pkey"
        );
    }

    @Override
    protected Set<PackageDefinitionBusiness> getSetDaemon(DatabaseConnection db) {
        return Collections.emptySet();
    }

    @Override
    protected Set<PackageDefinitionBusiness> getSetBusiness(DatabaseConnection db) throws RemoteException, SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<PackageDefinitionBusiness>(HashCodeComparator.getInstance()),
            objectFactory,
            "select distinct\n"
            + "  pdb.pkey,\n"
            + "  pdb.package_definition,\n"
            + "  pdb.accounting,\n"
            + "  pdb.display,\n"
            + "  pdb.description,\n"
            + "  pdb.approved\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  package_definition_businesses pdb\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=pdb.accounting\n"
            + "order by\n"
            + "  pdb.pkey",
            connector.getConnectAs()
        );
    }
}
