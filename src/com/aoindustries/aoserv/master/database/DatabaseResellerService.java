/*
 * Copyright 2010-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.client.validator.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseResellerService extends DatabaseAccountTypeService<AccountingCode,Reseller> implements ResellerService {

    private final ObjectFactory<Reseller> objectFactory = new AutoObjectFactory<Reseller>(Reseller.class, connector);

    DatabaseResellerService(DatabaseConnector connector) {
        super(connector, AccountingCode.class, Reseller.class);
    }

    @Override
    protected List<Reseller> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Reseller>(),
            objectFactory,
            "select * from resellers"
        );
    }

    @Override
    protected List<Reseller> getListDaemon(DatabaseConnection db) {
        return Collections.emptyList();
    }

    @Override
    protected List<Reseller> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Reseller>(),
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
            connector.getSwitchUser()
        );
    }
}
