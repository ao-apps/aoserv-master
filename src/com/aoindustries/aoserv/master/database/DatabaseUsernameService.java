/*
 * Copyright 2009-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.client.validator.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseUsernameService extends DatabaseAccountTypeService<UserId,Username> implements UsernameService {

    // <editor-fold defaultstate="collapsed" desc="Data Access">
    private final ObjectFactory<Username> objectFactory = new AutoObjectFactory<Username>(Username.class, connector);

    DatabaseUsernameService(DatabaseConnector connector) {
        super(connector, UserId.class, Username.class);
    }

    @Override
    protected ArrayList<Username> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Username>(),
            objectFactory,
            "select * from usernames"
        );
    }

    @Override
    protected ArrayList<Username> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Username>(),
            objectFactory,
            "select distinct\n"
            + "  un.*\n"
            + "from\n"
            + "  master_servers ms\n"
            + "  left join ao_servers ff on ms.server=ff.failover_server,\n"
            + "  business_servers bs,\n"
            + "  usernames un\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and (\n"
            + "    ms.server=bs.server\n"
            + "    or ff.server=bs.server\n"
            + "  ) and bs.accounting=un.accounting",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<Username> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<Username>(),
            objectFactory,
            "select\n"
            + "  un2.*\n"
            + "from\n"
            + "  usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "  usernames un2\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and (\n"
            + "    un2.username=un1.username\n"
            + UN1_BU1_PARENTS_OR_WHERE
            + "  )\n"
            + "  and bu1.accounting=un2.accounting",
            connector.getConnectAs()
        );
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Commands">
    void setUsernamePassword(DatabaseConnection db, InvalidateSet invalidateSet, UserId username, String plaintext, boolean isInteractive) throws RemoteException, SQLException {
        // Cascade to specific account types
        Username un = connector.factory.rootConnector.getUsernames().get(username);
        for(AOServObject<?> dependent : un.getDependentObjects()) {
            if(dependent instanceof PasswordProtected) {
                ((PasswordProtected)dependent).getSetPasswordCommand(plaintext).execute(connector, isInteractive);
            }
        }
    }
    // </editor-fold>
}
