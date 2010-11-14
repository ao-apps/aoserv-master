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
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabasePostgresVersionService extends DatabasePublicService<Integer,PostgresVersion> implements PostgresVersionService {

    private final ObjectFactory<PostgresVersion> objectFactory = new AutoObjectFactory<PostgresVersion>(PostgresVersion.class, connector);

    DatabasePostgresVersionService(DatabaseConnector connector) {
        super(connector, Integer.class, PostgresVersion.class);
    }

    @Override
    protected ArrayList<PostgresVersion> getPublicList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<PostgresVersion>(),
            objectFactory,
            "select * from postgres_versions"
        );
    }
}
