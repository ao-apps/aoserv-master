package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.Shell;
import com.aoindustries.aoserv.client.ShellService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseShellService extends DatabaseServiceUnixPathKey<Shell> implements ShellService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<Shell> objectFactory = new AutoObjectFactory<Shell>(Shell.class, this);

    DatabaseShellService(DatabaseConnector connector) {
        super(connector, Shell.class);
    }

    protected Set<Shell> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from shells"
        );
    }

    protected Set<Shell> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from shells"
        );
    }

    protected Set<Shell> getSetBusiness() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from shells"
        );
    }
}
