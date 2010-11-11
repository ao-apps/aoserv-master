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
import com.aoindustries.util.ArraySet;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTechnologyService extends DatabasePublicService<Integer,Technology> implements TechnologyService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<Technology> objectFactory = new AutoObjectFactory<Technology>(Technology.class, this);

    DatabaseTechnologyService(DatabaseConnector connector) {
        super(connector, Integer.class, Technology.class);
    }

    @Override
    protected Set<Technology> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArraySet<Technology>(),
            objectFactory,
            "select * from technologies order by pkey"
        );
    }
}
