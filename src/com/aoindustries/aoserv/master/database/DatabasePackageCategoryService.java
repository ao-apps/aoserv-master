package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.PackageCategory;
import com.aoindustries.aoserv.client.PackageCategoryService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabasePackageCategoryService extends DatabaseServiceStringKey<PackageCategory> implements PackageCategoryService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<PackageCategory> objectFactory = new AutoObjectFactory<PackageCategory>(PackageCategory.class, this);

    DatabasePackageCategoryService(DatabaseConnector connector) {
        super(connector, PackageCategory.class);
    }

    @Override
    public Set<PackageCategory> getSet() throws RemoteException {
        try {
            return Collections.unmodifiableSet(
                connector.factory.database.executeObjectSetQuery(
                    objectFactory,
                    "select * from package_categories"
                )
            );
        } catch(IOException err) {
            throw new RemoteException(err.getMessage(), err);
        } catch(SQLException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }
}
