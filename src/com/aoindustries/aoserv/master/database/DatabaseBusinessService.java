package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.Business;
import com.aoindustries.aoserv.client.BusinessService;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseBusinessService extends DatabaseService<AccountingCode,Business> implements BusinessService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<Business> objectFactory = new AutoObjectFactory<Business>(Business.class, this);

    DatabaseBusinessService(DatabaseConnector connector) {
        super(connector, AccountingCode.class, Business.class);
    }

    @Override
    protected Set<Business> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<Business>(),
            objectFactory,
            "select * from businesses"
        );
    }

    @Override
    protected Set<Business> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<Business>(),
            objectFactory,
            "select distinct\n"
            + "  bu.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  business_servers bs,\n"
            + "  businesses bu\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=bs.server\n"
            + "  and bs.accounting=bu.accounting",
            connector.getConnectAs()
        );
    }

    @Override
    protected Set<Business> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<Business>(),
            objectFactory,
            "select\n"
            + "  bu1.*\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN_NO_COMMA
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )",
            connector.getConnectAs()
        );
    }
}
