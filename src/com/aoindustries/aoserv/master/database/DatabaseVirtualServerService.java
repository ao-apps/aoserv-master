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
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
final class DatabaseVirtualServerService extends DatabaseServerService<VirtualServer> implements VirtualServerService {

    private static final String SELECT_COLUMNS_COMMON =
        SERVER_SELECT_COLUMNS + ",\n"
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
            + "  vs.requires_hvm"
    ;

    private final ObjectFactory<VirtualServer> objectFactory = new AutoObjectFactory<VirtualServer>(VirtualServer.class, connector);

    DatabaseVirtualServerService(DatabaseConnector connector) {
        super(connector, VirtualServer.class);
    }

    @Override
    protected List<VirtualServer> getListMaster(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<VirtualServer>(),
            objectFactory,
            "select\n"
            + SELECT_COLUMNS_COMMON + ",\n"
            + "  vs.vnc_password\n"
            + "from\n"
            + "  virtual_servers vs\n"
            + "  inner join servers se on vs.resource=se.resource\n"
            + "  inner join resources re on se.resource=re.pkey"
        );
    }

    @Override
    protected List<VirtualServer> getListDaemon(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<VirtualServer>(),
            objectFactory,
            "select distinct\n"
            + SELECT_COLUMNS_COMMON + ",\n"
            + "  vs.vnc_password\n"
            + "from\n"
            + "  master_servers ms\n"
            + "  left join ao_servers ao on ms.server=ao.server\n"
            // Allow its failover parent
            + "  left join ao_servers ff on ao.failover_server=ff.server\n"
            // Allow its failover children
            + "  left join ao_servers fs on ao.server=fs.failover_server\n"
            // Allow servers it replicates to
            + "  left join failover_file_replications ffr on ms.server=ffr.server\n"
            + "  left join backup_partitions bp on ffr.backup_partition=bp.pkey,\n"
            + "  servers se,\n"
            + "  virtual_servers vs,\n"
            + "  resources re\n"
            + "where\n"
            + "  ms.username=?\n"
            + "  and (\n"
            // Allow direct access
            + "    ms.server=se.resource\n"
            // Allow its failover parent
            + "    or ff.server=se.resource\n"
            // Allow its failover children
            + "    or fs.server=se.resource\n"
            // Allow servers it replicates to
            + "    or bp.ao_server=se.resource\n"
            + "  ) and se.resource=vs.resource\n"
            + "  and vs.resource=re.pkey",
            connector.getSwitchUser()
        );
    }

    @Override
    protected List<VirtualServer> getListBusiness(DatabaseConnection db) throws SQLException {
        return db.executeObjectCollectionQuery(
            new ArrayList<VirtualServer>(),
            objectFactory,
            "select distinct\n"
            + SELECT_COLUMNS_COMMON + ",\n"
            + "  case\n"
            + "    when vs.vnc_password is null then null\n"
            // Only provide the password when the user can connect to VNC console
            + "    when (\n"
            + "      select bs2.pkey from business_servers bs2 where bs2.accounting=bs.accounting and bs2.server=vs.resource and bs2.can_vnc_console limit 1\n"
            + "    ) is not null then vs.vnc_password\n"
            + "    else ?\n"
            + "  end\n"
            + "from\n"
            + "  usernames un,\n"
            + "  business_servers bs,\n"
            // Allow servers it replicates to
            //+ "  left join failover_file_replications ffr on bs.server=ffr.server\n"
            //+ "  left join backup_partitions bp on ffr.backup_partition=bp.pkey,\n"
            + "  servers se,\n"
            + "  virtual_servers vs,\n"
            + "  resources re\n"
            + "where\n"
            + "  un.username=?\n"
            + "  and un.accounting=bs.accounting\n"
            + "  and (\n"
            + "    bs.server=se.resource\n"
            // Allow servers it replicates to
            //+ "    or bp.ao_server=se.resource\n"
            + "  ) and se.resource=vs.resource\n"
            + "  and vs.resource=re.pkey",
            AOServObject.FILTERED,
            connector.getSwitchUser()
        );
    }
}
