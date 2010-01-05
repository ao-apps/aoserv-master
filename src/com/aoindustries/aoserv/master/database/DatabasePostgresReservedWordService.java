package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.PostgresReservedWord;
import com.aoindustries.aoserv.client.PostgresReservedWordService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabasePostgresReservedWordService extends DatabaseServiceStringKey<PostgresReservedWord> implements PostgresReservedWordService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<PostgresReservedWord> objectFactory = new AutoObjectFactory<PostgresReservedWord>(PostgresReservedWord.class, this);

    DatabasePostgresReservedWordService(DatabaseConnector connector) {
        super(connector, PostgresReservedWord.class);
    }

    protected Set<PostgresReservedWord> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from postgres_reserved_words"
        );
    }

    protected Set<PostgresReservedWord> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from postgres_reserved_words"
        );
    }

    protected Set<PostgresReservedWord> getSetBusiness() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from postgres_reserved_words"
        );
    }
}
