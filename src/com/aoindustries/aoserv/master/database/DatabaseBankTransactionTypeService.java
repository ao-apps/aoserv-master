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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseBankTransactionTypeService extends DatabaseService<String,BankTransactionType> implements BankTransactionTypeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<BankTransactionType> objectFactory = new AutoObjectFactory<BankTransactionType>(BankTransactionType.class, this);

    DatabaseBankTransactionTypeService(DatabaseConnector connector) {
        super(connector, String.class, BankTransactionType.class);
    }

    @Override
    protected Set<BankTransactionType> getSetMaster(DatabaseConnection db) throws RemoteException, SQLException {
        if(connector.factory.rootConnector.getMasterUsers().get(connector.getConnectAs()).getCanAccessBankAccount()) {
            return db.executeObjectCollectionQuery(
                new HashSet<BankTransactionType>(),
                objectFactory,
                "select * from bank_transaction_types"
            );
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    protected Set<BankTransactionType> getSetDaemon(DatabaseConnection db) {
        return Collections.emptySet();
    }

    @Override
    protected Set<BankTransactionType> getSetBusiness(DatabaseConnection db) {
        return Collections.emptySet();
    }
}
