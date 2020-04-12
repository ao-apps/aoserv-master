/*
 * Copyright 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.infrastructure;

import com.aoindustries.aoserv.client.infrastructure.ServerFarm;
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
public class ServerFarm_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.SERVER_FARMS);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.SELECT,
			new ServerFarm(),
			"select * from infrastructure.\"ServerFarm\""
		);
	}

	@Override
	protected void getTableDaemon(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.SELECT,
			new ServerFarm(),
			"select distinct\n"
			+ "  sf.*\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms,\n"
			+ "            net.\"Host\"          se\n"
			+ "  left join backup.\"FileReplication\" ffr on  se.id             = ffr.server\n"
			+ "  left join backup.\"BackupPartition\" bp  on ffr.backup_partition =  bp.id\n"
			+ "  left join net.\"Host\"          fs  on  bp.ao_server        =  fs.id,\n"
			+ "  infrastructure.\"ServerFarm\" sf\n"
			+ "where\n"
			+ "  ms.username=?\n"
			+ "  and ms.server=se.id\n"
			+ "  and (\n"
			+ "    se.farm=sf.name\n"
			+ "    or fs.farm=sf.name\n"
			+ "  )",
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
			CursorMode.SELECT,
			new ServerFarm(),
			"select distinct\n"
			+ "  sf.*\n"
			+ "from\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk,\n"
			+ "  account.\"AccountHost\" bs,\n"
			+ "  net.\"Host\" se,\n"
			+ "  infrastructure.\"ServerFarm\" sf\n"
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk.name\n"
			+ "  and (\n"
			+ "    (\n"
			+ "      pk.accounting=bs.accounting\n"
			+ "      and bs.server=se.id\n"
			+ "      and se.farm=sf.name\n"
			+ "    ) or pk.id=sf.owner\n"
			+ "  )",
			source.getCurrentAdministrator()
		);
	}
}
