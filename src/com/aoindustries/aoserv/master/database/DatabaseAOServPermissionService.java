package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServPermission;
import com.aoindustries.aoserv.client.AOServPermissionService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseAOServPermissionService extends DatabaseService<String,AOServPermission> implements AOServPermissionService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<AOServPermission> objectFactory = new AutoObjectFactory<AOServPermission>(AOServPermission.class, this);

    DatabaseAOServPermissionService(DatabaseConnector connector) {
        super(connector, String.class, AOServPermission.class);
    }

    protected Set<AOServPermission> getSetMaster(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from aoserv_permissions"
        );
    }

    protected Set<AOServPermission> getSetDaemon(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from aoserv_permissions"
        );
    }

    protected Set<AOServPermission> getSetBusiness(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from aoserv_permissions"
        );
    }
}
