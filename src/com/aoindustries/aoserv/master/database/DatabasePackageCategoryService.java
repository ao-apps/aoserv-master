package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.PackageCategory;
import com.aoindustries.aoserv.client.PackageCategoryService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabasePackageCategoryService extends DatabaseService<String,PackageCategory> implements PackageCategoryService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<PackageCategory> objectFactory = new AutoObjectFactory<PackageCategory>(PackageCategory.class, this);

    DatabasePackageCategoryService(DatabaseConnector connector) {
        super(connector, String.class, PackageCategory.class);
    }

    protected Set<PackageCategory> getSetMaster(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from package_categories"
        );
    }

    protected Set<PackageCategory> getSetDaemon(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from package_categories"
        );
    }

    protected Set<PackageCategory> getSetBusiness(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from package_categories"
        );
    }
}
