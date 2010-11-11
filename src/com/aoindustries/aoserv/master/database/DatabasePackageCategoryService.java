/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabasePackageCategoryService extends DatabasePublicService<String,PackageCategory> implements PackageCategoryService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<PackageCategory> objectFactory = new AutoObjectFactory<PackageCategory>(PackageCategory.class, this);

    DatabasePackageCategoryService(DatabaseConnector connector) {
        super(connector, String.class, PackageCategory.class);
    }

    @Override
    protected Set<PackageCategory> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<PackageCategory>(),
            objectFactory,
            "select * from package_categories"
        );
    }
}
