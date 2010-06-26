package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.CreditCard;
import com.aoindustries.aoserv.client.CreditCardService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import com.aoindustries.util.ArraySet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseCreditCardService extends DatabaseService<Integer,CreditCard> implements CreditCardService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<CreditCard> objectFactory = new AutoObjectFactory<CreditCard>(CreditCard.class, this);

    DatabaseCreditCardService(DatabaseConnector connector) {
        super(connector, Integer.class, CreditCard.class);
    }

    @Override
    protected Set<CreditCard> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<CreditCard>(),
            objectFactory,
            "select * from credit_cards order by pkey"
        );
    }

    @Override
    protected Set<CreditCard> getSetDaemon(DatabaseConnection db) {
        return Collections.emptySet();
    }

    @Override
    protected Set<CreditCard> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<CreditCard>(),
            objectFactory,
            "select\n"
            + "  cc.*\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  credit_cards cc\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=cc.accounting\n"
            + "order by\n"
            + "  pkey",
            connector.getConnectAs()
        );
    }
}
