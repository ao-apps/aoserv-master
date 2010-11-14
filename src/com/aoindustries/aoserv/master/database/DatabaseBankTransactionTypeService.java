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
final class DatabaseBankTransactionTypeService extends DatabaseService<String,BankTransactionType> implements BankTransactionTypeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<BankTransactionType> objectFactory = new AutoObjectFactory<BankTransactionType>(BankTransactionType.class, connector);

    DatabaseBankTransactionTypeService(DatabaseConnector connector) {
        super(connector, String.class, BankTransactionType.class);
    }

    @Override
    protected ArrayList<BankTransactionType> getListMaster(DatabaseConnection db) throws RemoteException, SQLException {
        if(connector.factory.rootConnector.getMasterUsers().get(connector.getConnectAs()).getCanAccessBankAccount()) {
            return db.executeObjectCollectionQuery(
                new ArrayList<BankTransactionType>(),
                objectFactory,
                "select * from bank_transaction_types"
            );
        } else {
            return new ArrayList<BankTransactionType>(0);
        }
    }

    @Override
    protected ArrayList<BankTransactionType> getListDaemon(DatabaseConnection db) {
        return new ArrayList<BankTransactionType>(0);
    }

    @Override
    protected ArrayList<BankTransactionType> getListBusiness(DatabaseConnection db) {
        return new ArrayList<BankTransactionType>(0);
    }
}
