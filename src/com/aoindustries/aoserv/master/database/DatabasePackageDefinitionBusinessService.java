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
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabasePackageDefinitionBusinessService extends DatabaseAccountTypeService<Integer,PackageDefinitionBusiness> implements PackageDefinitionBusinessService {

    private final ObjectFactory<PackageDefinitionBusiness> objectFactory = new AutoObjectFactory<PackageDefinitionBusiness>(PackageDefinitionBusiness.class, connector);

    DatabasePackageDefinitionBusinessService(DatabaseConnector connector) {
        super(connector, Integer.class, PackageDefinitionBusiness.class);
    }

    @Override
    protected ArrayList<PackageDefinitionBusiness> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<PackageDefinitionBusiness>(),
            objectFactory,
            "select pkey, package_definition, accounting, display, description, approved from package_definition_businesses"
        );
    }

    @Override
    protected ArrayList<PackageDefinitionBusiness> getListDaemon(DatabaseConnection db) {
        return new ArrayList<PackageDefinitionBusiness>(0);
    }

    @Override
    protected ArrayList<PackageDefinitionBusiness> getListBusiness(DatabaseConnection db) throws RemoteException, SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<PackageDefinitionBusiness>(),
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
            + "  and bu1.accounting=pdb.accounting",
            connector.getSwitchUser()
        );
    }
}
