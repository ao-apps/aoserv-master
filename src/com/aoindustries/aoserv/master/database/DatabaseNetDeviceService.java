package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.NetDevice;
import com.aoindustries.aoserv.client.NetDeviceService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseNetDeviceService extends DatabaseServiceIntegerKey<NetDevice> implements NetDeviceService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<NetDevice> objectFactory = new AutoObjectFactory<NetDevice>(NetDevice.class, this);

    DatabaseNetDeviceService(DatabaseConnector connector) {
        super(connector, NetDevice.class);
    }

    protected Set<NetDevice> getSetMaster(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from net_devices"
        );
    }

    protected Set<NetDevice> getSetDaemon(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select distinct\n"
            + "  nd.*\n"
            + "from\n"
            + "  master_servers ms\n"
            + "  left join ao_servers ff on ms.server=ff.failover_server,\n"
            + "  net_devices nd\n"
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

    protected Set<NetDevice> getSetBusiness(DatabaseConnection db) throws IOException, SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select distinct\n"
            + "  nd.*\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            // Allow failover destinations
            //+ "  left outer join failover_file_replications ffr on bs.server=ffr.server\n"
            //+ "  left outer join backup_partitions bp on ffr.backup_partition=bp.pkey\n"
            //+ "  left outer join ao_servers bpao on bp.ao_server=bpao.server,\n"
            + "  net_devices nd\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs.accounting\n"
            + "  and (\n"
            + "    bs.server=nd.server\n"
            //+ "    or (bp.ao_server=nd.ao_server and nd.device_id=bpao.daemon_device_id)\n"
            + "  )",
            connector.getConnectAs()
        );
    }
}
