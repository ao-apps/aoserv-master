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
final class DatabaseTechnologyClassService extends DatabasePublicService<String,TechnologyClass> implements TechnologyClassService {

    private final ObjectFactory<TechnologyClass> objectFactory = new AutoObjectFactory<TechnologyClass>(TechnologyClass.class, connector);

    DatabaseTechnologyClassService(DatabaseConnector connector) {
        super(connector, String.class, TechnologyClass.class);
    }

    @Override
    protected ArrayList<TechnologyClass> getPublicList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<TechnologyClass>(),
            objectFactory,
            "select * from technology_classes"
        );
    }
}
