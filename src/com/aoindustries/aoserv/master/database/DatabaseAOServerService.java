package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServObject;
import com.aoindustries.aoserv.client.AOServer;
import com.aoindustries.aoserv.client.AOServerService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseAOServerService extends DatabaseServiceIntegerKey<AOServer> implements AOServerService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<AOServer> objectFactory = new AutoObjectFactory<AOServer>(AOServer.class, this);

    DatabaseAOServerService(DatabaseConnector connector) {
        super(connector, AOServer.class);
    }

    protected Set<AOServer> getSetMaster() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select * from ao_servers"
        );
    }

    protected Set<AOServer> getSetDaemon() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select distinct\n"
            + "  ao2.*\n"
            + "from\n"
            + "  master_servers ms\n"
            + "  inner join ao_servers ao on ms.server=ao.server\n"
            // Allow its failover parent
            + "  left join ao_servers ff on ao.failover_server=ff.server\n"
            // Allow its failover children
            + "  left join ao_servers fs on ao.server=fs.failover_server\n"
            // Allow servers it replicates to
            + "  left join failover_file_replications ffr on ms.server=ffr.server\n"
            + "  left join backup_partitions bp on ffr.backup_partition=bp.pkey,\n"
            + "  ao_servers ao2\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and (\n"
            // Allow direct access
            + "    ms.server=ao2.server\n"
            // Allow its failover parent
            + "    or ff.server=ao2.server\n"
            // Allow its failover children
            + "    or fs.server=ao2.server\n"
            // Allow servers it replicates to
            + "    or bp.ao_server=ao2.server\n"
            + "  )",
            connector.getConnectAs()
        );
    }

    protected Set<AOServer> getSetBusiness() throws IOException, SQLException {
        return connector.factory.database.executeObjectSetQuery(
            objectFactory,
            "select distinct\n"
            + "  ao.server,\n"
            + "  ao.hostname,\n"
            + "  null,\n" // daemon_bind
            + "  ?,\n"
            + "  ao.pool_size,\n"
            + "  ao.distro_hour,\n"
            + "  ao.last_distro_time,\n"
            + "  ao.failover_server,\n"
            + "  ao.daemon_device_id,\n"
            + "  null,\n" // daemon_connect_bind
            + "  ao.time_zone,\n"
            + "  null,\n" // jilter_bind
            + "  ao.restrict_outbound_email,\n"
            + "  ao.daemon_connect_address,\n"
            + "  ao.failover_batch_size,\n"
            + "  ao.monitoring_load_low,\n"
            + "  ao.monitoring_load_medium,\n"
            + "  ao.monitoring_load_high,\n"
            + "  ao.monitoring_load_critical\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            // Allow servers it replicates to
            //+ "  left join failover_file_replications ffr on bs.server=ffr.server\n"
            //+ "  left join backup_partitions bp on ffr.backup_partition=bp.pkey,\n"
            + "  ao_servers ao\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs.accounting\n"
            + "  and (\n"
            + "    bs.server=ao.server\n"
            // Allow servers it replicates to
            //+ "    or bp.ao_server=ao.server\n"
            + "  )",
            AOServObject.FILTERED,
            connector.getConnectAs()
        );
    }
}
