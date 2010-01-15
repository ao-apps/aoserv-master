package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServObject;
import com.aoindustries.aoserv.client.PostgresUser;
import com.aoindustries.aoserv.client.PostgresUserService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabasePostgresUserService extends DatabaseServiceIntegerKey<PostgresUser> implements PostgresUserService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<PostgresUser> objectFactory = new AutoObjectFactory<PostgresUser>(PostgresUser.class, this);

    DatabasePostgresUserService(DatabaseConnector connector) {
        super(connector, PostgresUser.class);
    }

    protected Set<PostgresUser> getSetMaster(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
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

    protected Set<PostgresUser> getSetDaemon(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
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

    protected Set<PostgresUser> getSetBusiness(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
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
}
