/*
 * Copyright 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.billing;

import com.aoindustries.aoserv.client.billing.WhoisHistoryAccount;
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
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class WhoisHistoryAccount_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.WhoisHistoryAccount);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new WhoisHistoryAccount(),
			"select * from billing.\"WhoisHistoryAccount\""
		);
	}

	@Override
	protected void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		// The servers don't need access to this information
		MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
	}

	@Override
	protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new WhoisHistoryAccount(),
			"select\n"
			+ "  wha.*\n"
			+ "from\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk,\n"
			+ TableHandler.BU1_PARENTS_JOIN
			+ "  billing.\"WhoisHistoryAccount\" wha\n"
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk.name\n"
			+ "  and (\n"
			+ TableHandler.PK_BU1_PARENTS_WHERE
			+ "  )\n"
			+ "  and bu1.accounting=wha.account",
			source.getCurrentAdministrator()
		);
	}
}
