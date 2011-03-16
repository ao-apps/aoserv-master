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

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseAOServPermissionService extends DatabaseService<String,AOServPermission> implements AOServPermissionService {

    private final ObjectFactory<AOServPermission> objectFactory = new AutoObjectFactory<AOServPermission>(AOServPermission.class, connector);

    DatabaseAOServPermissionService(DatabaseConnector connector) {
        super(connector, String.class, AOServPermission.class);
    }

    @Override
    protected ArrayList<AOServPermission> getList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<AOServPermission>(),
            objectFactory,
            "select * from aoserv_permissions"
        );
    }
}
