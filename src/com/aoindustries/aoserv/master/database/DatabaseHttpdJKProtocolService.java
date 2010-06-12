package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.HttpdJKProtocol;
import com.aoindustries.aoserv.client.HttpdJKProtocolService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseHttpdJKProtocolService extends DatabasePublicService<String,HttpdJKProtocol> implements HttpdJKProtocolService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<HttpdJKProtocol> objectFactory = new AutoObjectFactory<HttpdJKProtocol>(HttpdJKProtocol.class, this);

    DatabaseHttpdJKProtocolService(DatabaseConnector connector) {
        super(connector, String.class, HttpdJKProtocol.class);
    }

    protected Set<HttpdJKProtocol> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from httpd_jk_protocols"
        );
    }
}
