package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.Shell;
import com.aoindustries.aoserv.client.ShellService;
import com.aoindustries.aoserv.client.validator.UnixPath;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseShellService extends DatabaseService<UnixPath,Shell> implements ShellService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<Shell> objectFactory = new AutoObjectFactory<Shell>(Shell.class, this);

    DatabaseShellService(DatabaseConnector connector) {
        super(connector, UnixPath.class, Shell.class);
    }

    protected Set<Shell> getSetMaster(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from shells"
        );
    }

    protected Set<Shell> getSetDaemon(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from shells"
        );
    }

    protected Set<Shell> getSetBusiness(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from shells"
        );
    }
}
