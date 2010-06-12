package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.OperatingSystem;
import com.aoindustries.aoserv.client.OperatingSystemService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseOperatingSystemService extends DatabasePublicService<String,OperatingSystem> implements OperatingSystemService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<OperatingSystem> objectFactory = new AutoObjectFactory<OperatingSystem>(OperatingSystem.class, this);

    DatabaseOperatingSystemService(DatabaseConnector connector) {
        super(connector, String.class, OperatingSystem.class);
    }

    protected Set<OperatingSystem> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from operating_systems"
        );
    }
}
