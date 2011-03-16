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
final class DatabaseBankAccountService extends DatabaseBankAccountingService<String,BankAccount> implements BankAccountService {

    private final ObjectFactory<BankAccount> objectFactory = new AutoObjectFactory<BankAccount>(BankAccount.class, connector);

    DatabaseBankAccountService(DatabaseConnector connector) {
        super(connector, String.class, BankAccount.class);
    }

    @Override
    protected ArrayList<BankAccount> getListBankAccounting(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<BankAccount>(),
            objectFactory,
            "select * from bank_accounts"
        );
    }
}
