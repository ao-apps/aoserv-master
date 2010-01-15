package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.OperatingSystemVersion;
import com.aoindustries.aoserv.client.OperatingSystemVersionService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseOperatingSystemVersionService extends DatabaseServiceIntegerKey<OperatingSystemVersion> implements OperatingSystemVersionService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<OperatingSystemVersion> objectFactory = new AutoObjectFactory<OperatingSystemVersion>(OperatingSystemVersion.class, this);

    DatabaseOperatingSystemVersionService(DatabaseConnector connector) {
        super(connector, OperatingSystemVersion.class);
    }

    protected Set<OperatingSystemVersion> getSetMaster(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from operating_system_versions"
        );
    }

    protected Set<OperatingSystemVersion> getSetDaemon(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from operating_system_versions"
        );
    }

    protected Set<OperatingSystemVersion> getSetBusiness(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from operating_system_versions"
        );
    }
}
