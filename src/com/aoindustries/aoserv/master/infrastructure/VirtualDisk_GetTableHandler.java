/*
 * Copyright 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.infrastructure;

import com.aoindustries.aoserv.client.infrastructure.VirtualDisk;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.stream.StreamableOutput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class VirtualDisk_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.VIRTUAL_DISKS);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new VirtualDisk(),
			"select * from infrastructure.\"VirtualDisk\""
		);
	}

	@Override
	protected void getTableDaemon(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new VirtualDisk(),
			"select distinct\n"
			+ "  vd.*\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms\n"
			+ "  inner join infrastructure.\"VirtualDisk\" vd on ms.server=vd.virtual_server\n"
			+ "where\n"
			+ "  ms.username=?",
			source.getCurrentAdministrator()
		);
	}

	@Override
	protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new VirtualDisk(),
			"select distinct\n"
			+ "  vd.*\n"
			+ "from\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk,\n"
			+ "  account.\"AccountHost\" bs,\n"
			// Allow servers it replicates to
			//+ "  left join backup.\"FileReplication\" ffr on bs.server=ffr.server\n"
			//+ "  left join backup.\"BackupPartition\" bp on ffr.backup_partition=bp.id,\n"
			+ "  infrastructure.\"VirtualDisk\" vd\n"
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk.name\n"
			+ "  and pk.accounting=bs.accounting\n"
			+ "  and (\n"
			+ "    bs.server=vd.virtual_server\n"
			// Allow servers it replicates to
			//+ "    or bp.ao_server=vd.virtual_server\n"
			+ "  )",
			source.getCurrentAdministrator()
		);
	}
}
