package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.Protocol;
import com.aoindustries.aoserv.client.ProtocolService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseProtocolService extends DatabasePublicService<String,Protocol> implements ProtocolService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<Protocol> objectFactory = new AutoObjectFactory<Protocol>(Protocol.class, this);

    DatabaseProtocolService(DatabaseConnector connector) {
        super(connector, String.class, Protocol.class);
    }

    protected Set<Protocol> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from protocols"
        );
    }
}
