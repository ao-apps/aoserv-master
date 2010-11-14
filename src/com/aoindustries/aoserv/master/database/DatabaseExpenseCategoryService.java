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
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseExpenseCategoryService extends DatabaseService<String,ExpenseCategory> implements ExpenseCategoryService {

    private final ObjectFactory<ExpenseCategory> objectFactory = new AutoObjectFactory<ExpenseCategory>(ExpenseCategory.class, connector);

    DatabaseExpenseCategoryService(DatabaseConnector connector) {
        super(connector, String.class, ExpenseCategory.class);
    }

    @Override
    protected ArrayList<ExpenseCategory> getListMaster(DatabaseConnection db) throws RemoteException, SQLException {
        if(connector.factory.rootConnector.getMasterUsers().get(connector.getConnectAs()).getCanAccessBankAccount()) {
            return db.executeObjectCollectionQuery(
                new ArrayList<ExpenseCategory>(),
                objectFactory,
                "select * from expense_categories"
            );
        } else {
            return new ArrayList<ExpenseCategory>(0);
        }
    }

    @Override
    protected ArrayList<ExpenseCategory> getListDaemon(DatabaseConnection db) {
        return new ArrayList<ExpenseCategory>(0);
    }

    @Override
    protected ArrayList<ExpenseCategory> getListBusiness(DatabaseConnection db) {
        return new ArrayList<ExpenseCategory>(0);
    }
}
