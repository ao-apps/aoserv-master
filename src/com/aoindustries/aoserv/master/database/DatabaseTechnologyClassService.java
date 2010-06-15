package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.TechnologyClass;
import com.aoindustries.aoserv.client.TechnologyClassService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTechnologyClassService extends DatabasePublicService<String,TechnologyClass> implements TechnologyClassService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<TechnologyClass> objectFactory = new AutoObjectFactory<TechnologyClass>(TechnologyClass.class, this);

    DatabaseTechnologyClassService(DatabaseConnector connector) {
        super(connector, String.class, TechnologyClass.class);
    }

    protected Set<TechnologyClass> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from technology_classes"
        );
    }
}
