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
final class DatabaseExpenseCategoryService extends DatabaseBankAccountingService<String,ExpenseCategory> implements ExpenseCategoryService {

    private final ObjectFactory<ExpenseCategory> objectFactory = new AutoObjectFactory<ExpenseCategory>(ExpenseCategory.class, connector);

    DatabaseExpenseCategoryService(DatabaseConnector connector) {
        super(connector, String.class, ExpenseCategory.class);
    }

    @Override
    protected ArrayList<ExpenseCategory> getListBankAccounting(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<ExpenseCategory>(),
            objectFactory,
            "select * from expense_categories"
        );
    }
}
