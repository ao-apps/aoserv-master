package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.ProcessorType;
import com.aoindustries.aoserv.client.ProcessorTypeService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseProcessorTypeService extends DatabasePublicService<String,ProcessorType> implements ProcessorTypeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<ProcessorType> objectFactory = new AutoObjectFactory<ProcessorType>(ProcessorType.class, this);

    DatabaseProcessorTypeService(DatabaseConnector connector) {
        super(connector, String.class, ProcessorType.class);
    }

    protected Set<ProcessorType> getPublicSet(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from processor_types"
        );
    }
}
