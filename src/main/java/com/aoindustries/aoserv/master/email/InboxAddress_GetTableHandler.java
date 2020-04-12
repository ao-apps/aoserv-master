/*
 * Copyright 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.email;

import com.aoindustries.aoserv.client.email.InboxAddress;
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
public class InboxAddress_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.LINUX_ACC_ADDRESSES);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new InboxAddress(),
			"SELECT\n"
			+ "  ia.*,\n"
			// Protocol conversion <= 1.30:
			+ "  us.username\n"
			+ "FROM\n"
			+ "  email.\"InboxAddress\" ia\n"
			+ "  INNER JOIN linux.\"UserServer\" us ON ia.linux_server_account = us.id"
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
			new InboxAddress(),
			"SELECT\n"
			+ "  ia.*,\n"
			// Protocol conversion <= 1.30:
			+ "  us.username\n"
			+ "FROM\n"
			+ "  master.\"UserHost\"    ms,\n"
			+ "  email.\"Domain\"       ed,\n"
			+ "  email.\"Address\"      ea,\n"
			+ "  email.\"InboxAddress\" ia,\n"
			+ "  linux.\"UserServer\"   us\n"
			+ "WHERE\n"
			+ "      ms.username             = ?\n"
			+ "  AND ms.server               = ed.ao_server\n"
			+ "  AND ed.id                   = ea.domain\n"
			+ "  AND ea.id                   = ia.email_address\n"
			+ "  AND ia.linux_server_account = us.id",
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
			new InboxAddress(),
			"SELECT\n"
			+ "  ia.*,\n"
			// Protocol conversion <= 1.30:
			+ "  us.username\n"
			+ "FROM\n"
			+ "  account.\"User\"       un,\n"
			+ "  billing.\"Package\"    pk1,\n"
			+ TableHandler.BU1_PARENTS_JOIN
			+ "  billing.\"Package\"    pk2,\n"
			+ "  email.\"Domain\"       ed,\n"
			+ "  email.\"Address\"      ea,\n"
			+ "  email.\"InboxAddress\" ia,\n"
			+ "  linux.\"UserServer\"   us\n"
			+ "WHERE\n"
			+ "      un.username             = ?\n"
			+ "  AND un.package              = pk1.name\n"
			+ "  AND (\n"
			+ TableHandler.PK1_BU1_PARENTS_WHERE
			+ "  )\n"
			+ "  AND bu1.accounting          = pk2.accounting\n"
			+ "  AND pk2.name                = ed.package\n"
			+ "  AND ed.id                   = ea.domain\n"
			+ "  AND ea.id                   = ia.email_address\n"
			+ "  AND ia.linux_server_account = us.id",
			source.getCurrentAdministrator()
		);
	}
}
