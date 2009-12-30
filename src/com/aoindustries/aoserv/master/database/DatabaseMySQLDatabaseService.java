package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.MySQLDatabase;
import com.aoindustries.aoserv.client.MySQLDatabaseService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseMySQLDatabaseService extends DatabaseServiceIntegerKey<MySQLDatabase> implements MySQLDatabaseService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<MySQLDatabase> objectFactory = new AutoObjectFactory<MySQLDatabase>(MySQLDatabase.class, this);

    DatabaseMySQLDatabaseService(DatabaseConnector connector) {
        super(connector, MySQLDatabase.class);
    }

    protected Set<MySQLDatabase> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from mysql_databases"
        );
    }

    protected Set<MySQLDatabase> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  md.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  mysql_servers mys,\n"
            + "  mysql_databases md\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=mys.ao_server\n"
            + "  and mys.ao_server_resource=md.mysql_server",
            connector.getConnectAs()
        );
    }

    protected Set<MySQLDatabase> getSetBusiness() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select\n"
            + "  md.*\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  mysql_databases md\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=md.accounting",
            connector.getConnectAs()
        );
    }
}
