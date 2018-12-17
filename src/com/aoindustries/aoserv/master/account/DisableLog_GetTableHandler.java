/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.account;

import com.aoindustries.aoserv.client.account.DisableLog;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.CursorMode;
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
public class DisableLog_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.DISABLE_LOG);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new DisableLog(),
			"select * from account.\"DisableLog\""
		);
	}

	@Override
	protected void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new DisableLog(),
			"select distinct\n"
			+ "  dl.*\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms,\n"
			+ "  linux.\"Server\" ao\n"
			+ "  left join linux.\"Server\" ff on ao.server=ff.failover_server,\n"
			+ "  account.\"AccountHost\" bs,\n"
			+ "  account.\"DisableLog\" dl\n"
			+ "where\n"
			+ "  ms.username=?\n"
			+ "  and ms.server=ao.server\n"
			+ "  and (\n"
			+ "    ao.server=bs.server\n"
			+ "    or ff.server=bs.server\n"
			+ "  ) and bs.accounting=dl.accounting",
			source.getUsername()
		);
	}

	@Override
	protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new DisableLog(),
			"select\n"
			+ "  dl.*\n"
			+ "from\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk,\n"
			+ TableHandler.BU1_PARENTS_JOIN
			+ "  account.\"DisableLog\" dl\n"
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk.name\n"
			+ "  and (\n"
			+ TableHandler.PK_BU1_PARENTS_WHERE
			+ "  )\n"
			+ "  and bu1.accounting=dl.accounting",
			source.getUsername()
		);
	}
}
