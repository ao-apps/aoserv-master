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
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseTransactionTypeService extends DatabasePublicService<String,TransactionType> implements TransactionTypeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<TransactionType> objectFactory = new AutoObjectFactory<TransactionType>(TransactionType.class, connector);

    DatabaseTransactionTypeService(DatabaseConnector connector) {
        super(connector, String.class, TransactionType.class);
    }

    @Override
    protected ArrayList<TransactionType> getPublicList(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<TransactionType>(),
            objectFactory,
            "select * from transaction_types"
        );
    }
}
