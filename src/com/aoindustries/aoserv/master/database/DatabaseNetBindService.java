/*
 * Copyright 2009-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.database;

import com.aoindustries.aoserv.client.*;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseNetBindService extends DatabaseService<Integer,NetBind> implements NetBindService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<NetBind> objectFactory = new AutoObjectFactory<NetBind>(NetBind.class, this);

    DatabaseNetBindService(DatabaseConnector connector) {
        super(connector, Integer.class, NetBind.class);
    }

    @Override
    protected Set<NetBind> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<NetBind>(),
            objectFactory,
            "select\n"
            + "  pkey,\n"
            + "  business_server,\n"
            + "  ip_address,\n"
            + "  port,\n"
            + "  net_protocol,\n"
            + "  app_protocol,\n"
            + "  open_firewall,\n"
            + "  monitoring_enabled,\n"
            + "  monitoring_parameters\n"
            + "from\n"
            + "  net_binds"
        );
    }

    @Override
    protected Set<NetBind> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<NetBind>(),
            objectFactory,
            "select\n"
            + "  nb.pkey,\n"
            + "  nb.business_server,\n"
            + "  nb.ip_address,\n"
            + "  nb.port,\n"
            + "  nb.net_protocol,\n"
            + "  nb.app_protocol,\n"
            + "  nb.open_firewall,\n"
            + "  nb.monitoring_enabled,\n"
            + "  case when nb.monitoring_parameters is null then null::text else ? end as monitoring_parameters\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  servers se,\n"
            + "  net_binds nb\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=se.pkey\n"
            + "  and (\n"
            + "    ms.server=nb.server\n"
            + "    or (\n"
            + "      select\n"
            + "        ffr.pkey\n"
            + "      from\n"
            + "        failover_file_replications ffr\n"
            + "        inner join backup_partitions bp on ffr.backup_partition=bp.pkey\n"
            + "      where\n"
            + "        ms.server=ffr.server\n"
            + "        and bp.ao_server=nb.server\n"
            + "        and (\n"
            + "          nb.app_protocol=?\n"
            + "          or nb.app_protocol=?\n"
            + "        )\n"
            + "      limit 1\n"
            + "    ) is not null\n"
            + "  )",
            AOServObject.FILTERED,
            connector.getConnectAs(),
            Protocol.AOSERV_DAEMON,
            Protocol.AOSERV_DAEMON_SSL
        );
    }

    @Override
    protected Set<NetBind> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new HashSet<NetBind>(),
            objectFactory,
            "select\n"
            + "  nb.pkey,\n"
            + "  nb.business_server,\n"
            + "  nb.ip_address,\n"
            + "  nb.port,\n"
            + "  nb.net_protocol,\n"
            + "  nb.app_protocol,\n"
            + "  nb.open_firewall,\n"
            + "  nb.monitoring_enabled,\n"
            + "  case when nb.monitoring_parameters is null then null::text else ? end as monitoring_parameters\n"
            + "from\n"
            + "  net_binds nb\n"
            + "where\n"
            + "  nb.pkey in (\n"
            + "    select\n"
            + "      nb2.pkey\n"
            + "    from\n"
            + "      usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "      net_binds nb2\n"
            + "    where\n"
            + "      un1.username=?\n"
            + "      and (\n"
            + UN1_BU1_PARENTS_WHERE
            + "      )\n"
            + "      and bu1.accounting=nb2.accounting\n"
            + "  )\n"
            + "  or nb.pkey in (\n"
            + "    select\n"
            + "      nb3.pkey\n"
            + "    from\n"
            + "      usernames un3,\n"
            + BU2_PARENTS_JOIN
            + "      httpd_sites hs,\n"
            + "      httpd_site_binds hsb,\n"
            + "      net_binds nb3\n"
            + "    where\n"
            + "      un3.username=?\n"
            + "      and (\n"
            + UN3_BU2_PARENTS_WHERE
            + "      )\n"
            + "      and bu"+Business.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting=hs.accounting\n"
            + "      and hs.ao_server_resource=hsb.httpd_site\n"
            + "      and hsb.httpd_bind=nb3.pkey\n"
            + "  ) or nb.pkey in (\n"
            + "    select\n"
            + "      ms4.net_bind\n"
            + "    from\n"
            + "      usernames un4,\n"
            + "      business_servers bs4,\n"
            + "      mysql_servers ms4\n"
            + "    where\n"
            + "      un4.username=?\n"
            + "      and un4.accounting=bs4.accounting\n"
            + "      and bs4.server=ms4.ao_server\n"
            + "  ) or nb.pkey in (\n"
            + "    select\n"
            + "      ps5.net_bind\n"
            + "    from\n"
            + "      usernames un5,\n"
            + "      business_servers bs5,\n"
            + "      postgres_servers ps5\n"
            + "    where\n"
            + "      un5.username=?\n"
            + "      and un5.accounting=bs5.accounting\n"
            + "      and bs5.server=ps5.ao_server\n"
            /*+ "  ) or nb.pkey in (\n"
            // Allow net_binds of receiving failover_file_replications (exact package match - no tree inheritence)
            + "    select\n"
            + "      nb6.pkey\n"
            + "    from\n"
            + "      usernames un6,\n"
            + "      servers se6,\n"
            + "      failover_file_replications ffr6,\n"
            + "      backup_partitions bp6,\n"
            + "      net_binds nb6\n"
            + "    where\n"
            + "      un6.username=?\n"
            + "      and un6.accounting=se6.accounting\n"
            + "      and se6.pkey=ffr6.server\n"
            + "      and ffr6.backup_partition=bp6.pkey\n"
            + "      and bp6.ao_server=nb6.ao_server\n"
            + "      and (\n"
            + "        nb6.app_protocol='"+Protocol.AOSERV_DAEMON+"'\n"
            + "        or nb6.app_protocol='"+Protocol.AOSERV_DAEMON_SSL+"'\n"
            + "      )\n"*/
            + "  )",
            AOServObject.FILTERED,
            connector.getConnectAs(),
            connector.getConnectAs(),
            connector.getConnectAs(),
            connector.getConnectAs()//,
            //username
        );
    }
}
