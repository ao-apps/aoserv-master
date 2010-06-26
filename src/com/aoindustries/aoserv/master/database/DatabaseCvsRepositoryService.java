/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.CvsRepository;
import com.aoindustries.aoserv.client.CvsRepositoryService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import com.aoindustries.util.ArraySet;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseCvsRepositoryService extends DatabaseService<Integer,CvsRepository> implements CvsRepositoryService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<CvsRepository> objectFactory = new AutoObjectFactory<CvsRepository>(CvsRepository.class, this);

    DatabaseCvsRepositoryService(DatabaseConnector connector) {
        super(connector, Integer.class, CvsRepository.class);
    }

    @Override
    protected Set<CvsRepository> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<CvsRepository>(),
            objectFactory,
            "select ao_server_resource, path, linux_account_group, mode from cvs_repositories order by ao_server_resource"
        );
    }

    @Override
    protected Set<CvsRepository> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<CvsRepository>(),
            objectFactory,
            "select\n"
            + "  cr.ao_server_resource, cr.path, cr.linux_account_group, cr.mode\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  cvs_repositories cr\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=cr.ao_server\n"
            + "order by\n"
            + "  cr.ao_server_resource",
            connector.getConnectAs()
        );
    }

    @Override
    protected Set<CvsRepository> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            new ArraySet<CvsRepository>(),
            objectFactory,
            "select\n"
            + "  cr.ao_server_resource, cr.path, cr.linux_account_group, cr.mode\n"
            + "from\n"
            + "  usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "  ao_server_resources asr,\n"
            + "  cvs_repositories cr\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and (\n"
            + UN1_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=asr.accounting\n"
            + "  and asr.resource=cr.ao_server_resource\n"
            + "order by\n"
            + "  cr.ao_server_resource",
            connector.getConnectAs()
        );
    }
}
