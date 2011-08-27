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
final class DatabaseHttpdJKCodeService extends DatabaseService<String,HttpdJKCode> implements HttpdJKCodeService {

    private final ObjectFactory<HttpdJKCode> objectFactory = new AutoObjectFactory<HttpdJKCode>(HttpdJKCode.class, connector);

    DatabaseHttpdJKCodeService(DatabaseConnector connector) {
        super(connector, String.class, HttpdJKCode.class);
    }

    @Override
    protected List<HttpdJKCode> getList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<HttpdJKCode>(),
            objectFactory,
            "select * from httpd_jk_codes"
        );
    }
}
