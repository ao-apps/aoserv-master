/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.mysql;

import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.mysql.Server;
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
public class Server_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.MYSQL_SERVERS);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new Server(),
			"SELECT\n"
			+ "  ms.*,\n"
			// Protocol conversion
			+ "  (SELECT nb.package FROM net.\"Bind\" nb WHERE ms.bind = nb.id) AS \"packageName\"\n"
			+ "FROM\n"
			+ "  mysql.\"Server\" ms"
		);
	}

	@Override
	protected void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new Server(),
			"SELECT\n"
			+ "  ms.*,\n"
			// Protocol conversion
			+ "  (SELECT nb.package FROM net.\"Bind\" nb WHERE ms.bind = nb.id) AS \"packageName\"\n"
			+ "from\n"
			+ "             master.\"UserHost\" uh\n"
			+ "  INNER JOIN mysql.\"Server\"    ms ON uh.server=ms.ao_server\n"
			+ "where\n"
			+ "  uh.username=?",
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
			new Server(),
			"SELECT\n"
			+ "  ms.*,\n"
			// Protocol conversion
			+ "  (SELECT nb.package FROM net.\"Bind\" nb WHERE ms.bind = nb.id) AS \"packageName\"\n"
			+ "FROM\n"
			+ "             account.\"Username\"    un\n"
			+ "  INNER JOIN billing.\"Package\"     pk ON un.package    = pk.name\n"
			+ "  INNER JOIN account.\"AccountHost\" bs ON pk.accounting = bs.accounting\n"
			+ "  INNER JOIN mysql.\"Server\"        ms ON bs.server     = ms.ao_server\n"
			+ "WHERE\n"
			+ "  un.username=?",
			source.getUsername()
		);
	}
}
