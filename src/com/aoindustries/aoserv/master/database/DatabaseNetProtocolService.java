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
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseNetProtocolService extends DatabasePublicService<String,NetProtocol> implements NetProtocolService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<NetProtocol> objectFactory = new AutoObjectFactory<NetProtocol>(NetProtocol.class, this);

    DatabaseNetProtocolService(DatabaseConnector connector) {
        super(connector, String.class, NetProtocol.class);
    }

    @Override
    protected Set<NetProtocol> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<NetProtocol>(),
            objectFactory,
            "select * from net_protocols"
        );
    }
}
