package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.TechnologyName;
import com.aoindustries.aoserv.client.TechnologyNameService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTechnologyNameService extends DatabasePublicService<String,TechnologyName> implements TechnologyNameService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<TechnologyName> objectFactory = new AutoObjectFactory<TechnologyName>(TechnologyName.class, this);

    DatabaseTechnologyNameService(DatabaseConnector connector) {
        super(connector, String.class, TechnologyName.class);
    }

    @Override
    protected Set<TechnologyName> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<TechnologyName>(),
            objectFactory,
            "select * from technology_names"
        );
    }
}
