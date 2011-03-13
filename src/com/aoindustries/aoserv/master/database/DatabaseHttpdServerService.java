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
final class DatabaseHttpdServerService extends DatabaseService<Integer,HttpdServer> implements HttpdServerService {

    private final ObjectFactory<HttpdServer> objectFactory = new AutoObjectFactory<HttpdServer>(HttpdServer.class, connector);

    DatabaseHttpdServerService(DatabaseConnector connector) {
        super(connector, Integer.class, HttpdServer.class);
    }

    @Override
    protected ArrayList<HttpdServer> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<HttpdServer>(),
            objectFactory,
            "select\n"
            + DatabaseAOServerResourceService.SELECT_COLUMNS
            + "  hs.number,\n"
            + "  hs.max_binds,\n"
            + "  hs.linux_account_group,\n"
            + "  hs.mod_php_version,\n"
            + "  hs.use_suexec,\n"
            + "  hs.is_shared,\n"
            + "  hs.use_mod_perl,\n"
            + "  hs.timeout\n"
            + "from\n"
            + "  httpd_servers hs\n"
            + "  inner join ao_server_resources asr on hs.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey"
        );
    }

    @Override
    protected ArrayList<HttpdServer> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<HttpdServer>(),
            objectFactory,
            "select\n"
            + DatabaseAOServerResourceService.SELECT_COLUMNS
            + "  hs.number,\n"
            + "  hs.max_binds,\n"
            + "  hs.linux_account_group,\n"
            + "  hs.mod_php_version,\n"
            + "  hs.use_suexec,\n"
            + "  hs.is_shared,\n"
            + "  hs.use_mod_perl,\n"
            + "  hs.timeout\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  httpd_servers hs\n"
            + "  inner join ao_server_resources asr on hs.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=hs.ao_server",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<HttpdServer> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<HttpdServer>(),
            objectFactory,
            "select\n"
            + DatabaseAOServerResourceService.SELECT_COLUMNS
            + "  hs.number,\n"
            + "  hs.max_binds,\n"
            + "  hs.linux_account_group,\n"
            + "  hs.mod_php_version,\n"
            + "  hs.use_suexec,\n"
            + "  hs.is_shared,\n"
            + "  hs.use_mod_perl,\n"
            + "  hs.timeout\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            + "  httpd_servers hs\n"
            + "  inner join ao_server_resources asr on hs.ao_server_resource=asr.resource\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs.accounting\n"
            + "  and bs.server=hs.ao_server",
            connector.getConnectAs()
        );
    }
}
