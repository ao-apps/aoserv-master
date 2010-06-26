package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.CountryCode;
import com.aoindustries.aoserv.client.CountryCodeService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseCountryCodeService extends DatabasePublicService<String,CountryCode> implements CountryCodeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<CountryCode> objectFactory = new AutoObjectFactory<CountryCode>(CountryCode.class, this);

    DatabaseCountryCodeService(DatabaseConnector connector) {
        super(connector, String.class, CountryCode.class);
    }

    @Override
    protected Set<CountryCode> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<CountryCode>(),
            objectFactory,
            "select * from country_codes"
        );
    }
}
