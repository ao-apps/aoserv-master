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
final class DatabaseBankService extends DatabaseBankAccountingService<String,Bank> implements BankService {

    private final ObjectFactory<Bank> objectFactory = new AutoObjectFactory<Bank>(Bank.class, connector);

    DatabaseBankService(DatabaseConnector connector) {
        super(connector, String.class, Bank.class);
    }

    @Override
    protected ArrayList<Bank> getListBankAccounting(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Bank>(),
            objectFactory,
            "select * from banks"
        );
    }
}
