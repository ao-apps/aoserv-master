/*
 * Copyright 2010 by AO Industries, Inc.,
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
final class DatabaseProcessorTypeService extends DatabasePublicService<String,ProcessorType> implements ProcessorTypeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<ProcessorType> objectFactory = new AutoObjectFactory<ProcessorType>(ProcessorType.class, this);

    DatabaseProcessorTypeService(DatabaseConnector connector) {
        super(connector, String.class, ProcessorType.class);
    }

    @Override
    protected Set<ProcessorType> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<ProcessorType>(),
            objectFactory,
            "select * from processor_types"
        );
    }
}
