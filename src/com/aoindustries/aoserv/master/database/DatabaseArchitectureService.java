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
final class DatabaseArchitectureService extends DatabasePublicService<String,Architecture> implements ArchitectureService {

    private final ObjectFactory<Architecture> objectFactory = new AutoObjectFactory<Architecture>(Architecture.class, connector);

    DatabaseArchitectureService(DatabaseConnector connector) {
        super(connector, String.class, Architecture.class);
    }

    @Override
    protected ArrayList<Architecture> getPublicList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Architecture>(),
            objectFactory,
            "select * from architectures"
        );
    }
}
