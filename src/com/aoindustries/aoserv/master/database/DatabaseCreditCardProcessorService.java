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
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseCreditCardProcessorService extends DatabaseService<String,CreditCardProcessor> implements CreditCardProcessorService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<CreditCardProcessor> objectFactory = new AutoObjectFactory<CreditCardProcessor>(CreditCardProcessor.class, connector);

    DatabaseCreditCardProcessorService(DatabaseConnector connector) {
        super(connector, String.class, CreditCardProcessor.class);
    }

    @Override
    protected ArrayList<CreditCardProcessor> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<CreditCardProcessor>(),
            objectFactory,
            "select * from credit_card_processors"
        );
    }

    @Override
    protected ArrayList<CreditCardProcessor> getListDaemon(DatabaseConnection db) {
        return new ArrayList<CreditCardProcessor>(0);
    }

    @Override
    protected ArrayList<CreditCardProcessor> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<CreditCardProcessor>(),
            objectFactory,
            "select\n"
            + "  ccp.*\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  credit_card_processors ccp\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=ccp.accounting",
            connector.getConnectAs()
        );
    }
}
