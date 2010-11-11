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
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseFtpGuestUserService extends DatabaseService<Integer,FtpGuestUser> implements FtpGuestUserService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<FtpGuestUser> objectFactory = new AutoObjectFactory<FtpGuestUser>(FtpGuestUser.class, this);

    DatabaseFtpGuestUserService(DatabaseConnector connector) {
        super(connector, Integer.class, FtpGuestUser.class);
    }

    @Override
    protected Set<FtpGuestUser> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<FtpGuestUser>(),
            objectFactory,
            "select linux_account from ftp_guest_users"
        );
    }

    @Override
    protected Set<FtpGuestUser> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<FtpGuestUser>(),
            objectFactory,
            "select\n"
            + "  fgu.linux_account\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  linux_accounts la,\n"
            + "  ftp_guest_users fgu\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=la.ao_server\n"
            + "  and la.ao_server_resource=fgu.linux_account",
            connector.getConnectAs()
        );
    }

    @Override
    protected Set<FtpGuestUser> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<FtpGuestUser>(),
            objectFactory,
            "select\n"
            + "  fgu.linux_account\n"
            + "from\n"
            + "  usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "  linux_accounts la,\n"
            + "  ftp_guest_users fgu\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and (\n"
            + UN1_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=la.accounting\n"
            + "  and la.ao_server_resource=fgu.linux_account",
            connector.getConnectAs()
        );
    }
}
