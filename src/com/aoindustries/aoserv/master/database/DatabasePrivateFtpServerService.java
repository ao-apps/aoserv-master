/*
 * Copyright 2010 by AO Industries, Inc.,
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
final class DatabasePrivateFtpServerService extends DatabaseService<Integer,PrivateFtpServer> implements PrivateFtpServerService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<PrivateFtpServer> objectFactory = new AutoObjectFactory<PrivateFtpServer>(PrivateFtpServer.class, connector);

    DatabasePrivateFtpServerService(DatabaseConnector connector) {
        super(connector, Integer.class, PrivateFtpServer.class);
    }

    @Override
    protected ArrayList<PrivateFtpServer> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<PrivateFtpServer>(),
            objectFactory,
            "select\n"
            + DatabaseAOServerResourceService.SELECT_COLUMNS
            + "  pfs.net_bind,\n"
            + "  pfs.logfile,\n"
            + "  pfs.hostname,\n"
            + "  pfs.email,\n"
            + "  pfs.linux_account_group,\n"
            + "  pfs.allow_anonymous\n"
            + "from\n"
            + "  private_ftp_servers pfs\n"
            + "  inner join ao_server_resources asr on pfs.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey"
        );
    }

    @Override
    protected ArrayList<PrivateFtpServer> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<PrivateFtpServer>(),
            objectFactory,
            "select\n"
            + DatabaseAOServerResourceService.SELECT_COLUMNS
            + "  pfs.net_bind,\n"
            + "  pfs.logfile,\n"
            + "  pfs.hostname,\n"
            + "  pfs.email,\n"
            + "  pfs.linux_account_group,\n"
            + "  pfs.allow_anonymous\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  private_ftp_servers pfs\n"
            + "  inner join ao_server_resources asr on pfs.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=pfs.ao_server",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<PrivateFtpServer> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<PrivateFtpServer>(),
            objectFactory,
            "select\n"
            + DatabaseAOServerResourceService.SELECT_COLUMNS
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
            + "  inner join ao_server_resources asr on pfs.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
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
