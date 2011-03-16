/*
 * Copyright 2009-2011 by AO Industries, Inc.,
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

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseOperatingSystemService extends DatabaseService<String,OperatingSystem> implements OperatingSystemService {

    private final ObjectFactory<OperatingSystem> objectFactory = new AutoObjectFactory<OperatingSystem>(OperatingSystem.class, connector);

    DatabaseOperatingSystemService(DatabaseConnector connector) {
        super(connector, String.class, OperatingSystem.class);
    }

    @Override
    protected ArrayList<OperatingSystem> getList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<OperatingSystem>(),
            objectFactory,
            "select * from operating_systems"
        );
    }
}
