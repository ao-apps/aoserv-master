package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.CreditCardProcessor;
import com.aoindustries.aoserv.client.CreditCardProcessorService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseCreditCardProcessorService extends DatabaseService<String,CreditCardProcessor> implements CreditCardProcessorService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<CreditCardProcessor> objectFactory = new AutoObjectFactory<CreditCardProcessor>(CreditCardProcessor.class, this);

    DatabaseCreditCardProcessorService(DatabaseConnector connector) {
        super(connector, String.class, CreditCardProcessor.class);
    }

    @Override
    protected Set<CreditCardProcessor> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<CreditCardProcessor>(),
            objectFactory,
            "select * from credit_card_processors"
        );
    }

    @Override
    protected Set<CreditCardProcessor> getSetDaemon(DatabaseConnection db) {
        return Collections.emptySet();
    }

    @Override
    protected Set<CreditCardProcessor> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<CreditCardProcessor>(),
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
