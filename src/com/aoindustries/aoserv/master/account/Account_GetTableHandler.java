/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.account;

import com.aoindustries.aoserv.client.account.Account;
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
public class Account_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.BUSINESSES);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new Account(),
			"select * from account.\"Account\""
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
			new Account(),
			"select distinct\n"
			+ "  bu.*\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms,\n"
			+ "  account.\"AccountHost\" bs,\n"
			+ "  account.\"Account\" bu\n"
			+ "where\n"
			+ "  ms.username=?\n"
			+ "  and ms.server=bs.server\n"
			+ "  and bs.accounting=bu.accounting",
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
			new Account(),
			"WITH RECURSIVE accounts(accounting) AS (\n"
			+ "  SELECT\n"
			+ "    ac.*\n"
			+ "  FROM\n"
			+ "               account.\"User\"    un\n"
			+ "    INNER JOIN billing.\"Package\" pk ON un.package    = pk.name\n"
			+ "    INNER JOIN account.\"Account\" ac ON pk.accounting = ac.accounting\n"
			+ "  WHERE\n"
			+ "    un.username=?\n"
			+ "UNION ALL\n"
			+ "  SELECT a.* FROM\n"
			+ "    accounts\n"
			+ "    INNER JOIN account.\"Account\" a ON accounts.accounting = a.parent\n"
			+ ")\n"
			+ "SELECT * FROM accounts",
			source.getUsername()
		);
		/*
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new Account(),
			"select\n"
			+ "  bu1.*\n"
			+ "from\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk,\n"
			+ TableHandler.BU1_PARENTS_JOIN_NO_COMMA
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk.name\n"
			+ "  and (\n"
			+ TableHandler.PK_BU1_PARENTS_WHERE
			+ "  )",
			source.getUsername()
		);
		 */
	}
}
