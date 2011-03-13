/*
 * Copyright 2010-2011 by AO Industries, Inc.,
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
final class DatabaseHttpdJKProtocolService extends DatabasePublicService<String,HttpdJKProtocol> implements HttpdJKProtocolService {

    private final ObjectFactory<HttpdJKProtocol> objectFactory = new AutoObjectFactory<HttpdJKProtocol>(HttpdJKProtocol.class, connector);

    DatabaseHttpdJKProtocolService(DatabaseConnector connector) {
        super(connector, String.class, HttpdJKProtocol.class);
    }

    @Override
    protected ArrayList<HttpdJKProtocol> getPublicList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<HttpdJKProtocol>(),
            objectFactory,
            "select * from httpd_jk_protocols"
        );
    }
}
