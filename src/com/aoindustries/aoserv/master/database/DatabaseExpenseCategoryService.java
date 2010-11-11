package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseExpenseCategoryService extends DatabaseService<String,ExpenseCategory> implements ExpenseCategoryService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<ExpenseCategory> objectFactory = new AutoObjectFactory<ExpenseCategory>(ExpenseCategory.class, this);

    DatabaseExpenseCategoryService(DatabaseConnector connector) {
        super(connector, String.class, ExpenseCategory.class);
    }

    @Override
    protected Set<ExpenseCategory> getSetMaster(DatabaseConnection db) throws RemoteException, SQLException {
        if(connector.factory.rootConnector.getMasterUsers().get(connector.getConnectAs()).getCanAccessBankAccount()) {
            return db.executeObjectCollectionQuery(
                new HashSet<ExpenseCategory>(),
                objectFactory,
                "select * from expense_categories"
            );
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    protected Set<ExpenseCategory> getSetDaemon(DatabaseConnection db) {
        return Collections.emptySet();
    }

    @Override
    protected Set<ExpenseCategory> getSetBusiness(DatabaseConnection db) {
        return Collections.emptySet();
    }
}
