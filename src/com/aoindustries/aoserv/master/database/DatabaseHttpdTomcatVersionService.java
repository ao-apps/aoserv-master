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
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseHttpdTomcatVersionService extends DatabaseService<Integer,HttpdTomcatVersion> implements HttpdTomcatVersionService {

    private final ObjectFactory<HttpdTomcatVersion> objectFactory = new AutoObjectFactory<HttpdTomcatVersion>(HttpdTomcatVersion.class, connector);

    DatabaseHttpdTomcatVersionService(DatabaseConnector connector) {
        super(connector, Integer.class, HttpdTomcatVersion.class);
    }

    @Override
    protected List<HttpdTomcatVersion> getList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<HttpdTomcatVersion>(),
            objectFactory,
            "select * from httpd_tomcat_versions"
        );
    }
}
