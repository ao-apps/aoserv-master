/*
 * Copyright 2010-2011 by AO Industries, Inc.,
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
final class DatabaseProcessorTypeService extends DatabasePublicService<String,ProcessorType> implements ProcessorTypeService {

    private final ObjectFactory<ProcessorType> objectFactory = new AutoObjectFactory<ProcessorType>(ProcessorType.class, connector);

    DatabaseProcessorTypeService(DatabaseConnector connector) {
        super(connector, String.class, ProcessorType.class);
    }

    @Override
    protected ArrayList<ProcessorType> getPublicList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<ProcessorType>(),
            objectFactory,
            "select * from processor_types"
        );
    }
}
