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
final class DatabaseResourceTypeService extends DatabasePublicService<String,ResourceType> implements ResourceTypeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<ResourceType> objectFactory = new AutoObjectFactory<ResourceType>(ResourceType.class, connector);

    DatabaseResourceTypeService(DatabaseConnector connector) {
        super(connector, String.class, ResourceType.class);
    }

    @Override
    protected ArrayList<ResourceType> getPublicList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<ResourceType>(),
            objectFactory,
            "select * from resource_types"
        );
    }
}
