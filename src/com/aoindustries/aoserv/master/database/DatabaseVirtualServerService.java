package com.aoindustries.aoserv.master.database;

/*
 * Copyright 2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServObject;
import com.aoindustries.aoserv.client.VirtualServer;
import com.aoindustries.aoserv.client.VirtualServerService;
import com.aoindustries.sql.AutoObjectFactory;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseVirtualServerService extends DatabaseService<Integer,VirtualServer> implements VirtualServerService<DatabaseConnector,DatabaseConnectorFactory> {

    private final ObjectFactory<VirtualServer> objectFactory = new AutoObjectFactory<VirtualServer>(VirtualServer.class, this);

    DatabaseVirtualServerService(DatabaseConnector connector) {
        super(connector, Integer.class, VirtualServer.class);
    }

    protected Set<VirtualServer> getSetMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select * from virtual_servers"
        );
    }

    protected Set<VirtualServer> getSetDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select distinct\n"
            + "  vs.*\n"
            + "from\n"
            + "  master_servers ms\n"
            + "  inner join virtual_servers vs on ms.server=vs.server\n"
            + "where\n"
            + "  ms.username=?",
            connector.getConnectAs()
        );
    }

    protected Set<VirtualServer> getSetBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectSetQuery(
            objectFactory,
            "select distinct\n"
            + "  vs.server,\n"
            + "  vs.primary_ram,\n"
            + "  vs.primary_ram_target,\n"
            + "  vs.secondary_ram,\n"
            + "  vs.secondary_ram_target,\n"
            + "  vs.minimum_processor_type,\n"
            + "  vs.minimum_processor_architecture,\n"
            + "  vs.minimum_processor_speed,\n"
            + "  vs.minimum_processor_speed_target,\n"
            + "  vs.processor_cores,\n"
            + "  vs.processor_cores_target,\n"
            + "  vs.processor_weight,\n"
            + "  vs.processor_weight_target,\n"
            + "  vs.primary_physical_server_locked,\n"
            + "  vs.secondary_physical_server_locked,\n"
            + "  vs.requires_hvm,\n"
            + "  case\n"
            + "    when vs.vnc_password is null then null\n"
            // Only provide the password when the user can connect to VNC console
            + "    when (\n"
            + "      select bs2.pkey from business_servers bs2 where bs2.accounting=bs.accounting and bs2.server=vs.server and bs2.can_vnc_console limit 1\n"
            + "    ) is not null then vs.vnc_password\n"
            + "    else ?\n"
            + "  end\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            // Allow servers it replicates to
            //+ "  left join failover_file_replications ffr on bs.server=ffr.server\n"
            //+ "  left join backup_partitions bp on ffr.backup_partition=bp.pkey,\n"
            + "  virtual_servers vs\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs.accounting\n"
            + "  and (\n"
            + "    bs.server=vs.server\n"
            // Allow servers it replicates to
            //+ "    or bp.ao_server=vs.server\n"
            + "  )",
            AOServObject.FILTERED,
            connector.getConnectAs()
        );
    }
}
