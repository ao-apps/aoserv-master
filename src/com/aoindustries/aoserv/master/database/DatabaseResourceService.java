package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.Business;
import com.aoindustries.aoserv.client.Resource;
import com.aoindustries.aoserv.client.ResourceService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseResourceService extends DatabaseServiceIntegerKey<Resource> implements ResourceService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<Resource> objectFactory = new AutoObjectFactory<Resource>(Resource.class, this);

    DatabaseResourceService(DatabaseConnector connector) {
        super(connector, Resource.class);
    }

    protected Set<Resource> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from resources"
        );
    }

    protected Set<Resource> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            // ao_server_resources
            "select\n"
            + "  re.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  ao_server_resources asr,\n"
            + "  resources re\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=asr.ao_server\n"
            + "  and asr.resource=re.pkey\n"
            // server_resources
            + "union select\n"
            + "  re.*\n"
            + "from\n"
            + "  master_servers ms,\n"
            + "  server_resources sr,\n"
            + "  resources re\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and ms.server=sr.server\n"
            + "  and sr.resource=re.pkey",
            connector.getConnectAs(),
            connector.getConnectAs()
        );
    }

    protected Set<Resource> getSetBusiness() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            // owns the resource
            "select\n"
            + "  re.*\n"
            + "from\n"
            + "  usernames un,\n"
            + BU1_PARENTS_JOIN
            + "  resources re\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and (\n"
            + UN_BU1_PARENTS_WHERE
            + "  )\n"
            + "  and bu1.accounting=re.accounting\n"
            // has access to the mysql_servers
            + "union select\n"
            + "  re.*\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            + "  mysql_servers ms,\n"
            + "  resources re\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs.accounting\n"
            + "  and bs.server=ms.ao_server\n"
            + "  and ms.ao_server_resource=re.pkey\n"
            // ip_addresses
            + "union select\n"
            + "  re.*\n"
            + "from\n"
            + "  resources re\n"
            + "where\n"
            + "  re.pkey in (\n"
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
            + "  or re.pkey in (\n"
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
            + "  ) or re.pkey in (\n"
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
            connector.getConnectAs(),
            connector.getConnectAs(),
            connector.getConnectAs()//,
            //connector.getConnectAs()
        );
    }
}
