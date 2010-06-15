package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.PostgresEncoding;
import com.aoindustries.aoserv.client.PostgresEncodingService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabasePostgresEncodingService extends DatabasePublicService<Integer,PostgresEncoding> implements PostgresEncodingService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<PostgresEncoding> objectFactory = new AutoObjectFactory<PostgresEncoding>(PostgresEncoding.class, this);

    DatabasePostgresEncodingService(DatabaseConnector connector) {
        super(connector,Integer.class,  PostgresEncoding.class);
    }

    protected Set<PostgresEncoding> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from postgres_encodings"
        );
    }
}
