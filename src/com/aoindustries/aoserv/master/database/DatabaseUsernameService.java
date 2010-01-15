package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.Username;
import com.aoindustries.aoserv.client.UsernameService;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseUsernameService extends DatabaseService<UserId,Username> implements UsernameService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<Username> objectFactory = new AutoObjectFactory<Username>(Username.class, this);

    DatabaseUsernameService(DatabaseConnector connector) {
        super(connector, UserId.class, Username.class);
    }

    protected Set<Username> getSetMaster(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from usernames"
        );
    }

    protected Set<Username> getSetDaemon(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
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

    protected Set<Username> getSetBusiness(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
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
}
