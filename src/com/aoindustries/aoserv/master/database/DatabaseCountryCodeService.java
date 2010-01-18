package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.CountryCode;
import com.aoindustries.aoserv.client.CountryCodeService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseCountryCodeService extends DatabasePublicService<String,CountryCode> implements CountryCodeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<CountryCode> objectFactory = new AutoObjectFactory<CountryCode>(CountryCode.class, this);

    DatabaseCountryCodeService(DatabaseConnector connector) {
        super(connector, String.class, CountryCode.class);
    }

    protected Set<CountryCode> getPublicSet(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from country_codes"
        );
    }
}
