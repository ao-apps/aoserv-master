package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.PostgresDatabase;
import com.aoindustries.aoserv.client.PostgresDatabaseService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabasePostgresDatabaseService extends DatabaseService<Integer,PostgresDatabase> implements PostgresDatabaseService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<PostgresDatabase> objectFactory = new AutoObjectFactory<PostgresDatabase>(PostgresDatabase.class, this);

    DatabasePostgresDatabaseService(DatabaseConnector connector) {
        super(connector, Integer.class, PostgresDatabase.class);
    }

    protected Set<PostgresDatabase> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  ao_server_resource,\n"
            + "  name,\n"
            + "  postgres_server,\n"
            + "  datdba,\n"
            + "  encoding,\n"
            + "  is_template,\n"
            + "  allow_conn,\n"
            + "  enable_postgis\n"
            + "from\n"
            + "  postgres_databases"
        );
    }

    protected Set<PostgresDatabase> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  pd.ao_server_resource,\n"
            + "  pd.name,\n"
            + "  pd.postgres_server,\n"
            + "  pd.datdba,\n"
            + "  pd.encoding,\n"
            + "  pd.is_template,\n"
            + "  pd.allow_conn,\n"
            + "  pd.enable_postgis\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  postgres_databases pd\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=pd.ao_server",
            connector.getConnectAs()
        );
    }

    protected Set<PostgresDatabase> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  pd.ao_server_resource,\n"
            + "  pd.name,\n"
            + "  pd.postgres_server,\n"
            + "  pd.datdba,\n"
            + "  pd.encoding,\n"
            + "  pd.is_template,\n"
            + "  pd.allow_conn,\n"
            + "  pd.enable_postgis\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  ao_server_resources aor,\n"
            + "  postgres_databases pd\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=aor.accounting\n"
            + "  and aor.resource=pd.ao_server_resource",
            connector.getConnectAs()
        );
    }
}
