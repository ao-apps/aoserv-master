/*
 * Copyright 2009-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.aoserv.master.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabasePostgresUserService extends DatabaseAOServerResourceService<PostgresUser> implements PostgresUserService {

    // <editor-fold defaultstate="collapsed" desc="Data Access">
    private final ObjectFactory<PostgresUser> objectFactory = new AutoObjectFactory<PostgresUser>(PostgresUser.class, connector);

    DatabasePostgresUserService(DatabaseConnector connector) {
        super(connector, PostgresUser.class);
    }

    @Override
    protected ArrayList<PostgresUser> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<PostgresUser>(),
            objectFactory,
            "select\n"
            + AOSERVER_RESOURCE_SELECT_COLUMNS + ",\n"
            + "  pu.username,\n"
            + "  pu.postgres_server,\n"
            + "  pu.createdb,\n"
            + "  pu.trace,\n"
            + "  pu.super,\n"
            + "  pu.catupd,\n"
            + "  pu.predisable_password\n"
            + "from\n"
            + "  postgres_users pu\n"
            + "  inner join ao_server_resources asr on pu.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey"
        );
    }

    @Override
    protected ArrayList<PostgresUser> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<PostgresUser>(),
            objectFactory,
            "select\n"
            + AOSERVER_RESOURCE_SELECT_COLUMNS + ",\n"
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
            + "  inner join ao_server_resources asr on pu.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=pu.ao_server",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<PostgresUser> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<PostgresUser>(),
            objectFactory,
             "select\n"
            + AOSERVER_RESOURCE_SELECT_COLUMNS + ",\n"
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
            + "  inner join ao_server_resources asr on pu.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
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
    void setPostgresUserPassword(DatabaseConnection db, InvalidateSet invalidateSet, int postgresUser, String plaintext) throws RemoteException, SQLException {
        try {
            PostgresUser mu = connector.factory.rootConnector.getPostgresUsers().get(postgresUser);
            DaemonHandler.getDaemonConnector(mu.getAoServer()).setPostgresUserPassword(postgresUser, plaintext);
        } catch(IOException err) {
            throw new RemoteException(err.getMessage(), err);
        }
    }
    // </editor-fold>
}
