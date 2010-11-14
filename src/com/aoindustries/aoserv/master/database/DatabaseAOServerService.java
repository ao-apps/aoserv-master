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
import java.util.ArrayList;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseAOServerService extends DatabaseService<Integer,AOServer> implements AOServerService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<AOServer> objectFactory = new AutoObjectFactory<AOServer>(AOServer.class, connector);

    DatabaseAOServerService(DatabaseConnector connector) {
        super(connector, Integer.class, AOServer.class);
    }

    @Override
    protected ArrayList<AOServer> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<AOServer>(),
            objectFactory,
            "select\n"
            + "  server,\n"
            + "  hostname,\n"
            + "  daemon_bind,\n"
            + "  daemon_key,\n"
            + "  pool_size,\n"
            + "  distro_hour,\n"
            + "  (extract(epoch from last_distro_time)*1000)::int8 as last_distro_time,\n"
            + "  failover_server,\n"
            + "  daemon_device_id,\n"
            + "  daemon_connect_bind,\n"
            + "  time_zone,\n"
            + "  jilter_bind,\n"
            + "  restrict_outbound_email,\n"
            + "  daemon_connect_address,\n"
            + "  failover_batch_size,\n"
            + "  monitoring_load_low,\n"
            + "  monitoring_load_medium,\n"
            + "  monitoring_load_high,\n"
            + "  monitoring_load_critical\n"
            + "from\n"
            + "  ao_servers"
        );
    }

    @Override
    protected ArrayList<AOServer> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<AOServer>(),
            objectFactory,
            "select distinct\n"
            + "  ao2.server,\n"
            + "  ao2.hostname,\n"
            + "  ao2.daemon_bind,\n"
            + "  ao2.daemon_key,\n"
            + "  ao2.pool_size,\n"
            + "  ao2.distro_hour,\n"
            + "  (extract(epoch from ao2.last_distro_time)*1000)::int8 as last_distro_time,\n"
            + "  ao2.failover_server,\n"
            + "  ao2.daemon_device_id,\n"
            + "  ao2.daemon_connect_bind,\n"
            + "  ao2.time_zone,\n"
            + "  ao2.jilter_bind,\n"
            + "  ao2.restrict_outbound_email,\n"
            + "  ao2.daemon_connect_address,\n"
            + "  ao2.failover_batch_size,\n"
            + "  ao2.monitoring_load_low,\n"
            + "  ao2.monitoring_load_medium,\n"
            + "  ao2.monitoring_load_high,\n"
            + "  ao2.monitoring_load_critical\n"
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

    @Override
    protected ArrayList<AOServer> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<AOServer>(),
            objectFactory,
            "select distinct\n"
            + "  ao.server,\n"
            + "  ao.hostname,\n"
            + "  null,\n" // daemon_bind
            + "  ?,\n"
            + "  ao.pool_size,\n"
            + "  ao.distro_hour,\n"
            + "  (extract(epoch from ao.last_distro_time)*1000)::int8 as last_distro_time,\n"
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
