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
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTimeZoneService extends DatabasePublicService<String,TimeZone> implements TimeZoneService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<TimeZone> objectFactory = new AutoObjectFactory<TimeZone>(TimeZone.class, connector);

    DatabaseTimeZoneService(DatabaseConnector connector) {
        super(connector, String.class, TimeZone.class);
    }

    @Override
    protected ArrayList<TimeZone> getPublicList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<TimeZone>(),
            objectFactory,
            "select * from time_zones"
        );
    }
}
