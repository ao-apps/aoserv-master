package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.Architecture;
import com.aoindustries.aoserv.client.ArchitectureService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseArchitectureService extends DatabaseServiceStringKey<Architecture> implements ArchitectureService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<Architecture> objectFactory = new AutoObjectFactory<Architecture>(Architecture.class, this);

    DatabaseArchitectureService(DatabaseConnector connector) {
        super(connector, Architecture.class);
    }

    protected Set<Architecture> getSetMaster(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from architectures"
        );
    }

    protected Set<Architecture> getSetDaemon(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from architectures"
        );
    }

    protected Set<Architecture> getSetBusiness(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from architectures"
        );
    }
}
