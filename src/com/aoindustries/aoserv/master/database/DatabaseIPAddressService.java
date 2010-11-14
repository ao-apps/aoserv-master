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
final class DatabaseIPAddressService extends DatabaseService<Integer,IPAddress> implements IPAddressService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<IPAddress> objectFactory = new AutoObjectFactory<IPAddress>(IPAddress.class, connector);

    DatabaseIPAddressService(DatabaseConnector connector) {
        super(connector, Integer.class, IPAddress.class);
    }

    @Override
    protected ArrayList<IPAddress> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<IPAddress>(),
            objectFactory,
            "select\n"
            + DatabaseServerResourceService.SELECT_COLUMNS
            + "  ia.ip_address,\n"
            + "  ia.net_device,\n"
            + "  ia.is_alias,\n"
            + "  ia.hostname,\n"
            + "  ia.available,\n"
            + "  ia.is_overflow,\n"
            + "  ia.is_dhcp,\n"
            + "  ia.ping_monitor_enabled,\n"
            + "  ia.external_ip_address,\n"
            + "  ia.netmask\n"
            + "from\n"
            + "  ip_addresses ia\n"
            + "  inner join server_resources sr on ia.server_resource=sr.resource\n"
            + "  inner join business_servers bs on sr.accounting=bs.accounting and sr.server=bs.server\n"
            + "  inner join resources re on sr.resource=re.pkey"
        );
    }

    @Override
    protected ArrayList<IPAddress> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<IPAddress>(),
            objectFactory,
            "select distinct\n"
            + DatabaseServerResourceService.SELECT_COLUMNS
            + "  ia.ip_address,\n"
            + "  ia.net_device,\n"
            + "  ia.is_alias,\n"
            + "  ia.hostname,\n"
            + "  ia.available,\n"
            + "  ia.is_overflow,\n"
            + "  ia.is_dhcp,\n"
            + "  ia.ping_monitor_enabled,\n"
            + "  ia.external_ip_address,\n"
            + "  ia.netmask\n"
            + "from\n"
            + "  master_servers ms\n"
            + "  left join ao_servers ff on ms.server=ff.failover_server,\n"
            + "  net_devices nd\n"
            + "  right outer join ip_addresses ia on nd.pkey=ia.net_device\n"
            + "  inner join server_resources sr on ia.server_resource=sr.resource\n"
            + "  inner join business_servers bs on sr.accounting=bs.accounting and sr.server=bs.server\n"
            + "  inner join resources re on sr.resource=re.pkey\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and (\n"
            + "    ms.server=nd.server\n"
            + "    or ff.server=nd.server\n"
            + "    or (\n"
            + "      select\n"
            + "        ffr.pkey\n"
            + "      from\n"
            + "        failover_file_replications ffr\n"
            + "        inner join backup_partitions bp on ffr.backup_partition=bp.pkey\n"
            + "        inner join ao_servers bpao on bp.ao_server=bpao.server\n" // Only allow access to the device device ID for failovers
            + "      where\n"
            + "        ms.server=ffr.server\n"
            + "        and bp.ao_server=nd.server\n"
            + "        and bpao.daemon_device_id=nd.device_id\n" // Only allow access to the device device ID for failovers
            + "      limit 1\n"
            + "    ) is not null\n"
            + "  )",
            connector.getConnectAs()
        );
    }

    @Override
    protected ArrayList<IPAddress> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<IPAddress>(),
            objectFactory,
            "select\n"
            + DatabaseServerResourceService.SELECT_COLUMNS
            + "  ia.ip_address,\n"
            + "  ia.net_device,\n"
            + "  ia.is_alias,\n"
            + "  ia.hostname,\n"
            + "  ia.available,\n"
            + "  ia.is_overflow,\n"
            + "  ia.is_dhcp,\n"
            + "  ia.ping_monitor_enabled,\n"
            + "  ia.external_ip_address,\n"
            + "  ia.netmask\n"
            + "from\n"
            + "  ip_addresses ia\n"
            + "  inner join server_resources sr on ia.server_resource=sr.resource\n"
            + "  inner join business_servers bs on sr.accounting=bs.accounting and sr.server=bs.server\n"
            + "  inner join resources re on sr.resource=re.pkey\n"
            + "where\n"
            + "  ia.server_resource in (\n"
            + "    select\n"
            + "      ia2.server_resource\n"
            + "    from\n"
            + "      usernames un1,\n"
            + BU1_PARENTS_JOIN
            + "      server_resources sr,\n"
            + "      ip_addresses ia2\n"
            + "    where\n"
            + "      un1.username=?\n"
            + "      and (\n"
            + UN1_BU1_PARENTS_WHERE
            + "      )\n"
            + "      and bu1.accounting=sr.accounting\n"
            + "      and sr.resource=ia2.server_resource\n"
            + "  )\n"
            + "  or ia.server_resource in (\n"
            + "    select\n"
            + "      nb.ip_address\n"
            + "    from\n"
            + "      usernames un3,\n"
            + BU2_PARENTS_JOIN
            + "      httpd_sites hs,\n"
            + "      httpd_site_binds hsb,\n"
            + "      net_binds nb\n"
            + "    where\n"
            + "      un3.username=?\n"
            + "      and (\n"
            + UN3_BU2_PARENTS_WHERE
            + "      )\n"
            + "      and bu"+Business.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting=hs.accounting\n"
            + "      and hs.ao_server_resource=hsb.httpd_site\n"
            + "      and hsb.httpd_bind=nb.pkey\n"
            + "  ) or ia.server_resource in (\n"
            + "    select\n"
            + "      ia5.server_resource\n"
            + "    from\n"
            + "      usernames un5,\n"
            + "      business_servers bs5,\n"
            + "      net_devices nd5,\n"
            + "      ip_addresses ia5\n"
            + "    where\n"
            + "      un5.username=?\n"
            + "      and un5.accounting=bs5.accounting\n"
            + "      and bs5.server=nd5.server\n"
            + "      and nd5.pkey=ia5.net_device\n"
            + "      and (ia5.ip_address like '127.%.%.%' or ip_address='::1' or ia5.is_overflow)\n"
            /*+ "  ) or ia.pkey in (\n"
            + "    select \n"
            + "      ia6.pkey\n"
            + "    from\n"
            + "      usernames un6,\n"
            + "      business_servers bs6,\n"
            + "      failover_file_replications ffr6,\n"
            + "      backup_partitions bp6,\n"
            + "      ao_servers ao6,\n"
            + "      net_devices nd6,\n"
            + "      ip_addresses ia6\n"
            + "    where\n"
            + "      un6.username=?\n"
            + "      and un6.accounting=bs6.accounting\n"
            + "      and bs6.server=ffr6.server\n"
            + "      and ffr6.backup_partition=bp6.pkey\n"
            + "      and bp6.ao_server=ao6.server\n"
            + "      and ao6.server=nd6.ao_server and ao6.daemon_device_id=nd6.device_id\n"
            + "      and nd6.pkey=ia6.net_device and not ia6.is_alias\n"*/
            + "  )",
            connector.getConnectAs(),
            connector.getConnectAs(),
            connector.getConnectAs()//,
            //connector.getConnectAs()
        );
    }
}
