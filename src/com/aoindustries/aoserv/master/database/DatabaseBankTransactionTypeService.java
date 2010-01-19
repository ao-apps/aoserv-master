package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.BankTransactionType;
import com.aoindustries.aoserv.client.BankTransactionTypeService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseBankTransactionTypeService extends DatabaseService<String,BankTransactionType> implements BankTransactionTypeService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<BankTransactionType> objectFactory = new AutoObjectFactory<BankTransactionType>(BankTransactionType.class, this);

    DatabaseBankTransactionTypeService(DatabaseConnector connector) {
        super(connector, String.class, BankTransactionType.class);
    }

    protected Set<BankTransactionType> getSetMaster(DatabaseConnection db) throws IOException, SQLException {
        if(connector.factory.rootConnector.getMasterUsers().get(connector.getConnectAs()).getCanAccessBankAccount()) {
            return db.executeObjectSetQuery(
                objectFactory,
                "select * from bank_transaction_types"
            );
        } else {
            return Collections.emptySet();
        }
    }

    protected Set<BankTransactionType> getSetDaemon(DatabaseConnection db) {
        return Collections.emptySet();
    }

    protected Set<BankTransactionType> getSetBusiness(DatabaseConnection db) {
        return Collections.emptySet();
    }
}
