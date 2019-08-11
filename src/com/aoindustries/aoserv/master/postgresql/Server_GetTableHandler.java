/*
 * Copyright 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.postgresql;

import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.postgresql.Server;
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
public class Server_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.POSTGRES_SERVERS);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new Server(),
			"SELECT\n"
			+ "  *\n"
			+ "FROM\n"
			+ "  postgresql.\"Server\""
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
			new Server(),
			"SELECT\n"
			+ "  ps.*\n"
			+ "FROM\n"
			+ "             master.\"UserHost\"   ms\n"
			+ "  INNER JOIN postgresql.\"Server\" ps ON ms.server = ps.ao_server\n"
			+ "WHERE\n"
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
			new Server(),
			"SELECT\n"
			+ "  ps.*\n"
			+ "FROM\n"
			+ "             account.\"User\"        un\n"
			+ "  INNER JOIN billing.\"Package\"     pk ON un.package    = pk.name\n"
			+ "  INNER JOIN account.\"AccountHost\" bs ON pk.accounting = bs.accounting\n"
			+ "  INNER JOIN postgresql.\"Server\"   ps ON bs.server     = ps.ao_server\n"
			+ "WHERE\n"
			+ "  un.username = ?",
			source.getCurrentAdministrator()
		);
	}
}
