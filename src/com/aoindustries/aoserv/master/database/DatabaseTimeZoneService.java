package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.TimeZone;
import com.aoindustries.aoserv.client.TimeZoneService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTimeZoneService extends DatabasePublicService<String,TimeZone> implements TimeZoneService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<TimeZone> objectFactory = new AutoObjectFactory<TimeZone>(TimeZone.class, this);

    DatabaseTimeZoneService(DatabaseConnector connector) {
        super(connector, String.class, TimeZone.class);
    }

    protected Set<TimeZone> getPublicSet(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from time_zones"
        );
    }
}
