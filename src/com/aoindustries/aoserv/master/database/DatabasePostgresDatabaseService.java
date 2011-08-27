/*
 * Copyright 2009-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabasePostgresDatabaseService extends DatabaseAOServerResourceService<PostgresDatabase> implements PostgresDatabaseService {

    private final ObjectFactory<PostgresDatabase> objectFactory = new AutoObjectFactory<PostgresDatabase>(PostgresDatabase.class, connector);

    DatabasePostgresDatabaseService(DatabaseConnector connector) {
        super(connector, PostgresDatabase.class);
    }

    @Override
    protected ArrayList<PostgresDatabase> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<PostgresDatabase>(),
            objectFactory,
            "select\n"
            + AOSERVER_RESOURCE_SELECT_COLUMNS + ",\n"
            + "  pd.name,\n"
            + "  pd.postgres_server,\n"
            + "  pd.datdba,\n"
            + "  pd.encoding,\n"
            + "  pd.is_template,\n"
            + "  pd.allow_conn,\n"
            + "  pd.enable_postgis\n"
            + "from\n"
            + "  postgres_databases pd\n"
            + "  inner join ao_server_resources asr on pd.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey"
        );
    }

    @Override
    protected ArrayList<PostgresDatabase> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<PostgresDatabase>(),
            objectFactory,
            "select\n"
            + AOSERVER_RESOURCE_SELECT_COLUMNS + ",\n"
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
            + "  inner join ao_server_resources asr on pd.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=pd.ao_server",
            connector.getSwitchUser()
        );
    }

    @Override
    protected ArrayList<PostgresDatabase> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<PostgresDatabase>(),
            objectFactory,
            "select\n"
            + AOSERVER_RESOURCE_SELECT_COLUMNS + ",\n"
            + "  pd.name,\n"
            + "  pd.postgres_server,\n"
            + "  pd.datdba,\n"
            + "  pd.encoding,\n"
            + "  pd.is_template,\n"
            + "  pd.allow_conn,\n"
            + "  pd.enable_postgis\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN_NO_COMMA
            + "  inner join ao_server_resources asr on bu1.accounting=asr.accounting\n"
            + "  inner join postgres_databases pd on asr.resource=pd.ao_server_resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )",
            connector.getSwitchUser()
        );
    }
}
