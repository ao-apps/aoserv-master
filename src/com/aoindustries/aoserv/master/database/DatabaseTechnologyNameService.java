/*
 * Copyright 2009-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTechnologyNameService extends DatabasePublicService<String,TechnologyName> implements TechnologyNameService {

    private final ObjectFactory<TechnologyName> objectFactory = new AutoObjectFactory<TechnologyName>(TechnologyName.class, connector);

    DatabaseTechnologyNameService(DatabaseConnector connector) {
        super(connector, String.class, TechnologyName.class);
    }

    @Override
    protected ArrayList<TechnologyName> getPublicList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<TechnologyName>(),
            objectFactory,
            "select * from technology_names"
        );
    }
}
