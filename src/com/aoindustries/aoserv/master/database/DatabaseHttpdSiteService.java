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
final class DatabaseHttpdSiteService extends DatabaseService<Integer,HttpdSite> implements HttpdSiteService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<HttpdSite> objectFactory = new AutoObjectFactory<HttpdSite>(HttpdSite.class, connector);

    DatabaseHttpdSiteService(DatabaseConnector connector) {
        super(connector, Integer.class, HttpdSite.class);
    }

    @Override
    protected ArrayList<HttpdSite> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<HttpdSite>(),
            objectFactory,
            "select\n"
            + DatabaseAOServerResourceService.SELECT_COLUMNS
            + "  hs.site_name,\n"
            + "  hs.list_first,\n"
            + "  hs.linux_account_group,\n"
            + "  hs.server_admin,\n"
            + "  hs.is_manual_config,\n"
            + "  hs.awstats_skip_files\n"
            + "from\n"
            + "  httpd_sites hs\n"
            + "  inner join ao_server_resources asr on hs.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey"
        );
    }

    @Override
    protected ArrayList<HttpdSite> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<HttpdSite>(),
            objectFactory,
            "select\n"
            + DatabaseAOServerResourceService.SELECT_COLUMNS
            + "  hs.site_name,\n"
            + "  hs.list_first,\n"
            + "  hs.linux_account_group,\n"
            + "  hs.server_admin,\n"
            + "  hs.is_manual_config,\n"
            + "  hs.awstats_skip_files\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  httpd_sites hs\n"
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
    protected ArrayList<HttpdSite> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<HttpdSite>(),
            objectFactory,
            "select\n"
            + DatabaseAOServerResourceService.SELECT_COLUMNS
            + "  hs.site_name,\n"
            + "  hs.list_first,\n"
            + "  hs.linux_account_group,\n"
            + "  hs.server_admin,\n"
            + "  hs.is_manual_config,\n"
            + "  hs.awstats_skip_files\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  httpd_sites hs\n"
            + "  inner join ao_server_resources asr on hs.ao_server_resource=asr.resource\n"
            + "  inner join business_servers bs on asr.accounting=bs.accounting and asr.ao_server=bs.server\n"
            + "  inner join resources re on asr.resource=re.pkey\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=hs.accounting",
            connector.getConnectAs()
        );
    }
}
