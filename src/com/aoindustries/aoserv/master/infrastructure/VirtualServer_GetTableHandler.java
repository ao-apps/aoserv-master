/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.infrastructure;

import com.aoindustries.aoserv.client.infrastructure.VirtualServer;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class VirtualServer_GetTableHandler implements TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.VIRTUAL_SERVERS);
	}

	@Override
	public void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new VirtualServer(),
			"select * from infrastructure.\"VirtualServer\""
		);
	}

	@Override
	public void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new VirtualServer(),
			"select distinct\n"
			+ "  vs.*\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms\n"
			+ "  inner join infrastructure.\"VirtualServer\" vs on ms.server=vs.server\n"
			+ "where\n"
			+ "  ms.username=?",
			source.getUsername()
		);
	}

	@Override
	public void getTableAdministrator(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new VirtualServer(),
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
			+ "      select bs2.id from account.\"AccountHost\" bs2 where bs2.accounting=pk.accounting and bs2.server=vs.server and bs2.can_vnc_console limit 1\n"
			+ "    ) is not null then vs.vnc_password\n"
			+ "    else '"+AoservProtocol.FILTERED+"'::text\n"
			+ "  end\n"
			+ "from\n"
			+ "  account.\"Username\" un,\n"
			+ "  billing.\"Package\" pk,\n"
			+ "  account.\"AccountHost\" bs,\n"
			// Allow servers it replicates to
			//+ "  left join backup.\"FileReplication\" ffr on bs.server=ffr.server\n"
			//+ "  left join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id,\n"
			+ "  infrastructure.\"VirtualServer\" vs\n"
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk.name\n"
			+ "  and pk.accounting=bs.accounting\n"
			+ "  and (\n"
			+ "    bs.server=vs.server\n"
			// Allow servers it replicates to
			//+ "    or bp.ao_server=vs.server\n"
			+ "  )",
			source.getUsername()
		);
	}
}
