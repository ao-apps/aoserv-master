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
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseOperatingSystemVersionService extends DatabaseService<Integer,OperatingSystemVersion> implements OperatingSystemVersionService {

    private final ObjectFactory<OperatingSystemVersion> objectFactory = new AutoObjectFactory<OperatingSystemVersion>(OperatingSystemVersion.class, connector);

    DatabaseOperatingSystemVersionService(DatabaseConnector connector) {
        super(connector, Integer.class, OperatingSystemVersion.class);
    }

    @Override
    protected List<OperatingSystemVersion> getList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<OperatingSystemVersion>(),
            objectFactory,
            "select * from operating_system_versions"
        );
    }
}
