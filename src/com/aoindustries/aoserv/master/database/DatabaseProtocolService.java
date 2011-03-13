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
final class DatabaseProtocolService extends DatabasePublicService<String,Protocol> implements ProtocolService {

    private final ObjectFactory<Protocol> objectFactory = new AutoObjectFactory<Protocol>(Protocol.class, connector);

    DatabaseProtocolService(DatabaseConnector connector) {
        super(connector, String.class, Protocol.class);
    }

    @Override
    protected ArrayList<Protocol> getPublicList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Protocol>(),
            objectFactory,
            "select * from protocols"
        );
    }
}
