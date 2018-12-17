/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.email;

import com.aoindustries.aoserv.client.email.CyrusImapdServer;
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
public class CyrusImapdServer_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.CYRUS_IMAPD_SERVERS);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new CyrusImapdServer(),
			"select * from email.\"CyrusImapdServer\""
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
			new CyrusImapdServer(),
			"select\n"
			+ "  cis.*\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms\n"
			+ "  inner join email.\"CyrusImapdServer\" cis on ms.server=cis.ao_server\n"
			+ "where\n"
			+ "  ms.username=?",
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
			new CyrusImapdServer(),
			"select\n"
			+ "  cis.*\n"
			+ "from\n"
			+ "             account.\"User\"           un\n"
			+ "  inner join billing.\"Package\"        pk  on un.package    = pk.name\n"
			+ "  inner join account.\"AccountHost\"    bs  on pk.accounting = bs.accounting\n"
			+ "  inner join email.\"CyrusImapdServer\" cis on bs.server     = cis.ao_server\n"
			+ "where\n"
			+ "  un.username=?",
			source.getUsername()
		);
	}
}
