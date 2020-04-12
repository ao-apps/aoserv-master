/*
 * Copyright 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.payment;

import com.aoindustries.aoserv.client.master.Permission;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.payment.Processor;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.stream.StreamableOutput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class Processor_GetTableHandler extends TableHandler.GetTableHandlerPermissionByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.CREDIT_CARD_PROCESSORS);
	}

	@Override
	protected Permission.Name getPermissionName() {
		return Permission.Name.get_credit_card_processors;
	}

	@Override
	protected void getTableMasterHasPermission(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.SELECT,
			new Processor(),
			"select * from payment.\"Processor\""
		);
	}

	@Override
	protected void getTableDaemonHasPermission(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		MasterServer.writeObjects(source, out, provideProgress, Collections.emptyList());
	}

	@Override
	protected void getTableAdministratorHasPermission(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.SELECT,
			new Processor(),
			"select\n"
			+ "  ccp.*\n"
			+ "from\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk,\n"
			+ TableHandler.BU1_PARENTS_JOIN
			+ "  payment.\"Processor\" ccp\n"
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk.name\n"
			+ "  and (\n"
			+ TableHandler.PK_BU1_PARENTS_WHERE
			+ "  )\n"
			+ "  and bu1.accounting=ccp.accounting",
			source.getCurrentAdministrator()
		);
	}
}
