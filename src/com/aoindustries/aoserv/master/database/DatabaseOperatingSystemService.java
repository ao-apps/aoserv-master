package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.OperatingSystem;
import com.aoindustries.aoserv.client.OperatingSystemService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseOperatingSystemService extends DatabaseServiceStringKey<OperatingSystem> implements OperatingSystemService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<OperatingSystem> objectFactory = new AutoObjectFactory<OperatingSystem>(OperatingSystem.class, this);

    DatabaseOperatingSystemService(DatabaseConnector connector) {
        super(connector, OperatingSystem.class);
    }

    protected Set<OperatingSystem> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from operating_systems"
        );
    }

    protected Set<OperatingSystem> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from operating_systems"
        );
    }

    protected Set<OperatingSystem> getSetBusiness() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from operating_systems"
        );
    }
}
