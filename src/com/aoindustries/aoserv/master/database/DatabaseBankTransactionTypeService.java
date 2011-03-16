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
final class DatabaseBankTransactionTypeService extends DatabaseBankAccountingService<String,BankTransactionType> implements BankTransactionTypeService {

    private final ObjectFactory<BankTransactionType> objectFactory = new AutoObjectFactory<BankTransactionType>(BankTransactionType.class, connector);

    DatabaseBankTransactionTypeService(DatabaseConnector connector) {
        super(connector, String.class, BankTransactionType.class);
    }

    @Override
    protected ArrayList<BankTransactionType> getListBankAccounting(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<BankTransactionType>(),
            objectFactory,
            "select * from bank_transaction_types"
        );
    }
}
