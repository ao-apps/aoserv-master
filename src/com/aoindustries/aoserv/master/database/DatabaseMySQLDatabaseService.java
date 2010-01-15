package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.MySQLDatabase;
import com.aoindustries.aoserv.client.MySQLDatabaseService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseMySQLDatabaseService extends DatabaseService<Integer,MySQLDatabase> implements MySQLDatabaseService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<MySQLDatabase> objectFactory = new AutoObjectFactory<MySQLDatabase>(MySQLDatabase.class, this);

    DatabaseMySQLDatabaseService(DatabaseConnector connector) {
        super(connector, Integer.class, MySQLDatabase.class);
    }

    protected Set<MySQLDatabase> getSetMaster(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select ao_server_resource, name, mysql_server from mysql_databases"
        );
    }

    protected Set<MySQLDatabase> getSetDaemon(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  md.ao_server_resource,\n"
            + "  md.name,\n"
            + "  md.mysql_server\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  mysql_databases md\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=md.ao_server",
            connector.getConnectAs()
        );
    }

    protected Set<MySQLDatabase> getSetBusiness(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  md.ao_server_resource,\n"
            + "  md.name,\n"
            + "  md.mysql_server\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  ao_server_resources aor,\n"
            + "  mysql_databases md\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=aor.accounting\n"
            + "  and aor.resource=md.ao_server_resource",
            connector.getConnectAs()
        );
    }
}
