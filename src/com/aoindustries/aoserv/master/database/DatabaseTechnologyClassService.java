package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.TechnologyClass;
import com.aoindustries.aoserv.client.TechnologyClassService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTechnologyClassService extends DatabaseServiceStringKey<TechnologyClass> implements TechnologyClassService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<TechnologyClass> objectFactory = new AutoObjectFactory<TechnologyClass>(TechnologyClass.class, this);

    DatabaseTechnologyClassService(DatabaseConnector connector) {
        super(connector, TechnologyClass.class);
    }

    protected Set<TechnologyClass> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from technology_classes"
        );
    }

    protected Set<TechnologyClass> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from technology_classes"
        );
    }

    protected Set<TechnologyClass> getSetBusiness() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from technology_classes"
        );
    }
}
