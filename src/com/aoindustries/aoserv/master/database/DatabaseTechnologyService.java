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
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTechnologyService extends DatabasePublicService<Integer,Technology> implements TechnologyService {

    private final ObjectFactory<Technology> objectFactory = new AutoObjectFactory<Technology>(Technology.class, connector);

    DatabaseTechnologyService(DatabaseConnector connector) {
        super(connector, Integer.class, Technology.class);
    }

    @Override
    protected ArrayList<Technology> getPublicList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Technology>(),
            objectFactory,
            "select * from technologies"
        );
    }
}
