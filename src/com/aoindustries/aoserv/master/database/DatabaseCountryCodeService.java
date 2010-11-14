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
final class DatabaseCountryCodeService extends DatabasePublicService<String,CountryCode> implements CountryCodeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<CountryCode> objectFactory = new AutoObjectFactory<CountryCode>(CountryCode.class, connector);

    DatabaseCountryCodeService(DatabaseConnector connector) {
        super(connector, String.class, CountryCode.class);
    }

    @Override
    protected ArrayList<CountryCode> getPublicList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<CountryCode>(),
            objectFactory,
            "select * from country_codes"
        );
    }
}
