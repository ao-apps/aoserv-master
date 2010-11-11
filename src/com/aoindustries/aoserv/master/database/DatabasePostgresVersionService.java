package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.PostgresVersion;
import com.aoindustries.aoserv.client.PostgresVersionService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabasePostgresVersionService extends DatabasePublicService<Integer,PostgresVersion> implements PostgresVersionService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<PostgresVersion> objectFactory = new AutoObjectFactory<PostgresVersion>(PostgresVersion.class, this);

    DatabasePostgresVersionService(DatabaseConnector connector) {
        super(connector, Integer.class, PostgresVersion.class);
    }

    @Override
    protected Set<PostgresVersion> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<PostgresVersion>(),
            objectFactory,
            "select * from postgres_versions"
        );
    }
}
