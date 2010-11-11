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
final class DatabasePaymentTypeService extends DatabasePublicService<String,PaymentType> implements PaymentTypeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<PaymentType> objectFactory = new AutoObjectFactory<PaymentType>(PaymentType.class, this);

    DatabasePaymentTypeService(DatabaseConnector connector) {
        super(connector, String.class, PaymentType.class);
    }

    @Override
    protected Set<PaymentType> getPublicSet(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<PaymentType>(),
            objectFactory,
            "select * from payment_types"
        );
    }
}
