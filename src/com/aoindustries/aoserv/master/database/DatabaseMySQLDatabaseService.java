/*
 * Copyright 2009-2010 by AO Industries, Inc.,
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
final class DatabaseMySQLDatabaseService extends DatabaseService<Integer,MySQLDatabase> implements MySQLDatabaseService {

    private final ObjectFactory<MySQLDatabase> objectFactory = new AutoObjectFactory<MySQLDatabase>(MySQLDatabase.class, connector);

    DatabaseMySQLDatabaseService(DatabaseConnector connector) {
        super(connector, Integer.class, MySQLDatabase.class);
    }

    @Override
    protected ArrayList<MySQLDatabase> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<MySQLDatabase>(),
            objectFactory,
            "select\n"
            + DatabaseAOServerResourceService.SELECT_COLUMNS
            + "  md.name,\n"
            + "  md.mysql_server\n"
            + "from\n"
            + "  mysql_databases md\n"
            + "  inner join ao_server_resources asr on md.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey"
        );
    }

    @Override
    protected ArrayList<MySQLDatabase> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<MySQLDatabase>(),
            objectFactory,
            "select\n"
            + DatabaseAOServerResourceService.SELECT_COLUMNS
            + "  md.name,\n"
            + "  md.mysql_server\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  mysql_databases md\n"
            + "  inner join ao_server_resources asr on md.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=md.ao_server",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<MySQLDatabase> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<MySQLDatabase>(),
            objectFactory,
            "select\n"
            + DatabaseAOServerResourceService.SELECT_COLUMNS
            + "  md.name,\n"
            + "  md.mysql_server\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN_NO_COMMA
            + "  inner join ao_server_resources asr on bu1.accounting=asr.accounting\n"
            + "  inner join mysql_databases md on asr.resource=md.ao_server_resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )",
            connector.getConnectAs()
        );
    }
}
