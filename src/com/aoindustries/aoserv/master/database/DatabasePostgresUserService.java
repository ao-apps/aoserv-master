/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.AOServObject;
import com.aoindustries.aoserv.client.PostgresUser;
import com.aoindustries.aoserv.client.PostgresUserService;
import com.aoindustries.aoserv.client.command.SetPostgresUserPasswordCommand;
import com.aoindustries.aoserv.master.DaemonHandler;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabasePostgresUserService extends DatabaseService<Integer,PostgresUser> implements PostgresUserService<DatabaseConnector,DatabaseConnectorFactory> {

    // <editor-fold defaultstate="collapsed" desc="Data Access">
    private final ObjectFactory<PostgresUser> objectFactory = new AutoObjectFactory<PostgresUser>(PostgresUser.class, this);

    DatabasePostgresUserService(DatabaseConnector connector) {
        super(connector, Integer.class, PostgresUser.class);
    }

    @Override
    protected Set<PostgresUser> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<PostgresUser>(),
            objectFactory,
            "select\n"
            + "  ao_server_resource,\n"
            + "  username,\n"
            + "  postgres_server,\n"
            + "  createdb,\n"
            + "  trace,\n"
            + "  super,\n"
            + "  catupd,\n"
            + "  predisable_password\n"
            + "from\n"
            + "  postgres_users"
        );
    }

    @Override
    protected Set<PostgresUser> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<PostgresUser>(),
            objectFactory,
            "select\n"
            + "  pu.ao_server_resource,\n"
            + "  pu.username,\n"
            + "  pu.postgres_server,\n"
            + "  pu.createdb,\n"
            + "  pu.trace,\n"
            + "  pu.super,\n"
            + "  pu.catupd,\n"
            + "  pu.predisable_password\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  postgres_users pu\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=pu.ao_server",
            connector.getConnectAs()
        );
    }

    @Override
    protected Set<PostgresUser> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<PostgresUser>(),
            objectFactory,
             "select\n"
            + "  pu.ao_server_resource,\n"
            + "  pu.username,\n"
            + "  pu.postgres_server,\n"
            + "  pu.createdb,\n"
            + "  pu.trace,\n"
            + "  pu.super,\n"
            + "  pu.catupd,\n"
            + "  case when pu.predisable_password is null then null else ? end\n"
            + "from\n"
            + "  usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "  postgres_users pu\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and (\n"
            + UN1_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=pu.accounting",
            AOServObject.FILTERED,
            connector.getConnectAs()
        );
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Commands">
    void setPostgresUserPassword(DatabaseConnection db, InvalidateSet invalidateSet, SetPostgresUserPasswordCommand command) throws RemoteException, SQLException {
        try {
            PostgresUser mu = connector.factory.rootConnector.getPostgresUsers().get(command.getPostgresUser());
            DaemonHandler.getDaemonConnector(mu.getAoServerResource().getAoServer()).setPostgresUserPassword(command.getPostgresUser(), command.getPlaintext());
        } catch(IOException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }
    // </editor-fold>
}
