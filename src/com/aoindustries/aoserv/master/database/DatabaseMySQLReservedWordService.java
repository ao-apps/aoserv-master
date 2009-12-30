package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.MySQLReservedWord;
import com.aoindustries.aoserv.client.MySQLReservedWordService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseMySQLReservedWordService extends DatabaseServiceStringKey<MySQLReservedWord> implements MySQLReservedWordService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<MySQLReservedWord> objectFactory = new AutoObjectFactory<MySQLReservedWord>(MySQLReservedWord.class, this);

    DatabaseMySQLReservedWordService(DatabaseConnector connector) {
        super(connector, MySQLReservedWord.class);
    }

    protected Set<MySQLReservedWord> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from mysql_reserved_words"
        );
    }

    protected Set<MySQLReservedWord> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from mysql_reserved_words"
        );
    }

    protected Set<MySQLReservedWord> getSetBusiness() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from mysql_reserved_words"
        );
    }
}
