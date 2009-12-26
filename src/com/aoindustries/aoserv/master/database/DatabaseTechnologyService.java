package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.Technology;
import com.aoindustries.aoserv.client.TechnologyService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTechnologyService extends DatabaseServiceIntegerKey<Technology> implements TechnologyService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<Technology> objectFactory = new AutoObjectFactory<Technology>(Technology.class, this);

    DatabaseTechnologyService(DatabaseConnector connector) {
        super(connector, Technology.class);
    }

    protected Set<Technology> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from technologies"
        );
    }

    protected Set<Technology> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from technologies"
        );
    }

    protected Set<Technology> getSetBusiness() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from technologies"
        );
    }
}
