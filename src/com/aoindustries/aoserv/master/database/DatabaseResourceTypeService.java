package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.ResourceType;
import com.aoindustries.aoserv.client.ResourceTypeService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseResourceTypeService extends DatabasePublicService<String,ResourceType> implements ResourceTypeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<ResourceType> objectFactory = new AutoObjectFactory<ResourceType>(ResourceType.class, this);

    DatabaseResourceTypeService(DatabaseConnector connector) {
        super(connector, String.class, ResourceType.class);
    }

    protected Set<ResourceType> getPublicSet(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from resource_types"
        );
    }
}
