package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.Reseller;
import com.aoindustries.aoserv.client.ResellerService;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseResellerService extends DatabaseService<AccountingCode,Reseller> implements ResellerService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<Reseller> objectFactory = new AutoObjectFactory<Reseller>(Reseller.class, this);

    DatabaseResellerService(DatabaseConnector connector) {
        super(connector, AccountingCode.class, Reseller.class);
    }

    @Override
    protected Set<Reseller> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from resellers"
        );
    }

    @Override
    protected Set<Reseller> getSetDaemon(DatabaseConnection db) {
        return Collections.emptySet();
    }

    @Override
    protected Set<Reseller> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  re.*\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  resellers re\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  ) and bu1.accounting=re.accounting",
            connector.getConnectAs()
        );
    }
}
