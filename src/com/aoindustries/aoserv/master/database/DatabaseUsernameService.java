/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.BusinessAdministrator;
import com.aoindustries.aoserv.client.LinuxAccount;
import com.aoindustries.aoserv.client.MySQLUser;
import com.aoindustries.aoserv.client.PostgresUser;
import com.aoindustries.aoserv.client.Username;
import com.aoindustries.aoserv.client.UsernameService;
import com.aoindustries.aoserv.client.command.SetBusinessAdministratorPasswordCommand;
import com.aoindustries.aoserv.client.command.SetUsernamePasswordCommand;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseUsernameService extends DatabaseService<UserId,Username> implements UsernameService<DatabaseConnector,DatabaseConnectorFactory> {

    // <editor-fold defaultstate="collapsed" desc="Data Access">
    private final ObjectFactory<Username> objectFactory = new AutoObjectFactory<Username>(Username.class, this);

    DatabaseUsernameService(DatabaseConnector connector) {
        super(connector, UserId.class, Username.class);
    }

    @Override
    protected Set<Username> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<Username>(),
            objectFactory,
            "select * from usernames"
        );
    }

    @Override
    protected Set<Username> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<Username>(),
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
            connector.connectAs
        );
    }

    @Override
    protected Set<Username> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<Username>(),
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
            connector.connectAs
        );
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Commands">
    public void setUsernamePassword(DatabaseConnection db, InvalidateSet invalidateSet, SetUsernamePasswordCommand command) throws RemoteException, SQLException {
        // Cascade to specific account types
        Username un = connector.factory.rootConnector.getUsernames().get(command.getUsername());
        // Make sure passes other command validations
        BusinessAdministrator ba = un.getBusinessAdministrator();
        if(ba!=null) {
            connector.businessAdministrators.setBusinessAdministratorPassword(
                db,
                invalidateSet,
                new SetBusinessAdministratorPasswordCommand(command.getUsername(), command.getPlaintext())
            );
        }
        for(LinuxAccount la : un.getLinuxAccounts()) {
            connector.linuxAccounts.setLinuxAccountPassword(
                db,
                invalidateSet,
                new SetLinuxAccountPasswordCommand(la.getKey(), command.getPlaintext())
            );
        }
        for(MySQLUser mu : un.getMysqlUsers()) {
            connector.mysqlUsers.setMySQLUserPassword(
                db,
                invalidateSet,
                new SetMySQLUserPasswordCommand(mu.getKey(), command.getPlaintext())
            );
        }
        for(PostgresUser pu : un.getPostgresUsers()) {
            connector.postgresUsers.setPostgresUserPassword(
                db,
                invalidateSet,
                new SetPostgresUserPasswordCommand(pu.getKey(), command.getPlaintext())
            );
        }
    }
    // </editor-fold>
}
