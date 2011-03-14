/*
 * Copyright 2010-2011 by AO Industries, Inc.,
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
final class DatabaseCvsRepositoryService extends DatabaseAOServerResourceService<CvsRepository> implements CvsRepositoryService {

    private final ObjectFactory<CvsRepository> objectFactory = new AutoObjectFactory<CvsRepository>(CvsRepository.class, connector);

    DatabaseCvsRepositoryService(DatabaseConnector connector) {
        super(connector, CvsRepository.class);
    }

    @Override
    protected ArrayList<CvsRepository> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<CvsRepository>(),
            objectFactory,
            "select\n"
            + AOSERVER_RESOURCE_SELECT_COLUMNS + ",\n"
            + "  cr.path,\n"
            + "  cr.linux_account_group,\n"
            + "  cr.mode\n"
            + "from\n"
            + "  cvs_repositories cr\n"
            + "  inner join ao_server_resources asr on cr.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey"
        );
    }

    @Override
    protected ArrayList<CvsRepository> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<CvsRepository>(),
            objectFactory,
            "select\n"
            + AOSERVER_RESOURCE_SELECT_COLUMNS + ",\n"
            + "  cr.path,\n"
            + "  cr.linux_account_group,\n"
            + "  cr.mode\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  cvs_repositories cr\n"
            + "  inner join ao_server_resources asr on cr.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=cr.ao_server",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<CvsRepository> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<CvsRepository>(),
            objectFactory,
            "select\n"
            + AOSERVER_RESOURCE_SELECT_COLUMNS + ",\n"
            + "  cr.path,\n"
            + "  cr.linux_account_group,\n"
            + "  cr.mode\n"
            + "from\n"
            + "  usernames un1,\n"
            + BU1_PARENTS_JOIN_NO_COMMA
            + "  inner join ao_server_resources asr on bu1.accounting=asr.accounting\n"
            + "  inner join cvs_repositories cr on asr.resource=cr.ao_server_resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
            + "where\n"
            + "  un1.username=?\n"
            + "  and (\n"
            + UN1_BU1_PARENTS_WHERE
            + "  )",
            connector.getConnectAs()
        );
    }
}
