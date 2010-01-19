package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.TransactionType;
import com.aoindustries.aoserv.client.TransactionTypeService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTransactionTypeService extends DatabasePublicService<String,TransactionType> implements TransactionTypeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<TransactionType> objectFactory = new AutoObjectFactory<TransactionType>(TransactionType.class, this);

    DatabaseTransactionTypeService(DatabaseConnector connector) {
        super(connector, String.class, TransactionType.class);
    }

    protected Set<TransactionType> getPublicSet(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from transaction_types"
        );
    }
}
