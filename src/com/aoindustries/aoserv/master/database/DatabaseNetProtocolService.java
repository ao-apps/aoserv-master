package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.NetProtocol;
import com.aoindustries.aoserv.client.NetProtocolService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseNetProtocolService extends DatabasePublicService<String,NetProtocol> implements NetProtocolService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<NetProtocol> objectFactory = new AutoObjectFactory<NetProtocol>(NetProtocol.class, this);

    DatabaseNetProtocolService(DatabaseConnector connector) {
        super(connector, String.class, NetProtocol.class);
    }

    protected Set<NetProtocol> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from net_protocols"
        );
    }
}
