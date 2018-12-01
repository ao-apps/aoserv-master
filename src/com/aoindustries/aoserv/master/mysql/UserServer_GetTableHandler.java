/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.mysql;

import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.mysql.UserServer;
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
public class UserServer_GetTableHandler implements TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.MYSQL_SERVER_USERS);
	}

	@Override
	public void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new UserServer(),
			"select * from mysql.\"UserServer\""
		);
	}

	@Override
	public void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new UserServer(),
			"select\n"
			+ "  msu.*\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms,\n"
			+ "  mysql.\"Server\" mys,\n"
			+ "  mysql.\"UserServer\" msu\n"
			+ "where\n"
			+ "  ms.username=?\n"
			+ "  and ms.server=mys.ao_server\n"
			+ "  and mys.bind=msu.mysql_server",
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
			new UserServer(),
			 "select\n"
			+ "  msu.id,\n"
			+ "  msu.username,\n"
			+ "  msu.mysql_server,\n"
			+ "  msu.host,\n"
			+ "  msu.disable_log,\n"
			+ "  case when msu.predisable_password is null then null else ? end,\n"
			+ "  msu.max_questions,\n"
			+ "  msu.max_updates\n,"
			+ "  msu.max_connections,\n"
			+ "  msu.max_user_connections\n"
			+ "from\n"
			+ "  account.\"Username\" un1,\n"
			+ "  billing.\"Package\" pk1,\n"
			+ TableHandler.BU1_PARENTS_JOIN
			+ "  billing.\"Package\" pk2,\n"
			+ "  account.\"Username\" un2,\n"
			+ "  mysql.\"UserServer\" msu\n"
			+ "where\n"
			+ "  un1.username=?\n"
			+ "  and un1.package=pk1.name\n"
			+ "  and (\n"
			+ TableHandler.PK1_BU1_PARENTS_WHERE
			+ "  )\n"
			+ "  and bu1.accounting=pk2.accounting\n"
			+ "  and pk2.name=un2.package\n"
			+ "  and un2.username=msu.username",
			AoservProtocol.FILTERED,
			source.getUsername()
		);
	}
}
