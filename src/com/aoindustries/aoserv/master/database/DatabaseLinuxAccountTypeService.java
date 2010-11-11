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
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseLinuxAccountTypeService extends DatabasePublicService<String,LinuxAccountType> implements LinuxAccountTypeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<LinuxAccountType> objectFactory = new AutoObjectFactory<LinuxAccountType>(LinuxAccountType.class, this);

    DatabaseLinuxAccountTypeService(DatabaseConnector connector) {
        super(connector, String.class, LinuxAccountType.class);
    }

    @Override
    protected Set<LinuxAccountType> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<LinuxAccountType>(),
            objectFactory,
            "select * from linux_account_types"
        );
    }
}
