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
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseLanguageService extends DatabasePublicService<String,Language> implements LanguageService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<Language> objectFactory = new AutoObjectFactory<Language>(Language.class, this);

    DatabaseLanguageService(DatabaseConnector connector) {
        super(connector, String.class, Language.class);
    }

    @Override
    protected Set<Language> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<Language>(),
            objectFactory,
            "select * from languages"
        );
    }
}
