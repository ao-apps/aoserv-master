/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.client.validator.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseShellService extends DatabasePublicService<UnixPath,Shell> implements ShellService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<Shell> objectFactory = new AutoObjectFactory<Shell>(Shell.class, this);

    DatabaseShellService(DatabaseConnector connector) {
        super(connector, UnixPath.class, Shell.class);
    }

    @Override
    protected Set<Shell> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<Shell>(),
            objectFactory,
            "select * from shells"
        );
    }
}
