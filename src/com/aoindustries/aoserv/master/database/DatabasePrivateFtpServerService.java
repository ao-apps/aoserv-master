package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.PrivateFtpServer;
import com.aoindustries.aoserv.client.PrivateFtpServerService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabasePrivateFtpServerService extends DatabaseService<Integer,PrivateFtpServer> implements PrivateFtpServerService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<PrivateFtpServer> objectFactory = new AutoObjectFactory<PrivateFtpServer>(PrivateFtpServer.class, this);

    DatabasePrivateFtpServerService(DatabaseConnector connector) {
        super(connector, Integer.class, PrivateFtpServer.class);
    }

    @Override
    protected Set<PrivateFtpServer> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<PrivateFtpServer>(),
            objectFactory,
            "select\n"
            + "  ao_server_resource,\n"
            + "  net_bind,\n"
            + "  logfile,\n"
            + "  hostname,\n"
            + "  email,\n"
            + "  linux_account_group,\n"
            + "  allow_anonymous\n"
            + "from\n"
            + "  private_ftp_servers"
        );
    }

    @Override
    protected Set<PrivateFtpServer> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<PrivateFtpServer>(),
            objectFactory,
            "select\n"
            + "  pfs.ao_server_resource,\n"
            + "  pfs.net_bind,\n"
            + "  pfs.logfile,\n"
            + "  pfs.hostname,\n"
            + "  pfs.email,\n"
            + "  pfs.linux_account_group,\n"
            + "  pfs.allow_anonymous\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  private_ftp_servers pfs\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=pfs.ao_server",
            connector.getConnectAs()
        );
    }

    @Override
    protected Set<PrivateFtpServer> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new HashSet<PrivateFtpServer>(),
            objectFactory,
            "select\n"
            + "  pfs.ao_server_resource,\n"
            + "  pfs.net_bind,\n"
            + "  pfs.logfile,\n"
            + "  pfs.hostname,\n"
            + "  pfs.email,\n"
            + "  pfs.linux_account_group,\n"
            + "  pfs.allow_anonymous\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  net_binds nb,\n"
            + "  private_ftp_servers pfs\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=pfs.accounting",
            connector.getConnectAs()
        );
    }
}
