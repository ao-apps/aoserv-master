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
final class DatabaseHttpdTomcatVersionService extends DatabasePublicService<Integer,HttpdTomcatVersion> implements HttpdTomcatVersionService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<HttpdTomcatVersion> objectFactory = new AutoObjectFactory<HttpdTomcatVersion>(HttpdTomcatVersion.class, this);

    DatabaseHttpdTomcatVersionService(DatabaseConnector connector) {
        super(connector, Integer.class, HttpdTomcatVersion.class);
    }

    @Override
    protected Set<HttpdTomcatVersion> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<HttpdTomcatVersion>(),
            objectFactory,
            "select * from httpd_tomcat_versions"
        );
    }
}
