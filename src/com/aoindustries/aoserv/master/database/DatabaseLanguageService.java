/*
 * Copyright 2009-2011 by AO Industries, Inc.,
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
final class DatabaseLanguageService extends DatabaseService<String,Language> implements LanguageService {

    private final ObjectFactory<Language> objectFactory = new AutoObjectFactory<Language>(Language.class, connector);

    DatabaseLanguageService(DatabaseConnector connector) {
        super(connector, String.class, Language.class);
    }

    @Override
    protected List<Language> getList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Language>(),
            objectFactory,
            "select * from languages"
        );
    }
}
