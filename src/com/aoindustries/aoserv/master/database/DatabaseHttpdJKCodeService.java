/*
 * Copyright 2010 by AO Industries, Inc.,
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
final class DatabaseHttpdJKCodeService extends DatabasePublicService<String,HttpdJKCode> implements HttpdJKCodeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<HttpdJKCode> objectFactory = new AutoObjectFactory<HttpdJKCode>(HttpdJKCode.class, this);

    DatabaseHttpdJKCodeService(DatabaseConnector connector) {
        super(connector, String.class, HttpdJKCode.class);
    }

    @Override
    protected Set<HttpdJKCode> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<HttpdJKCode>(),
            objectFactory,
            "select * from httpd_jk_codes"
        );
    }
}
