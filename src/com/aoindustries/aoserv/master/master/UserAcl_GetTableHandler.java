/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.master;

import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserAcl;
import com.aoindustries.aoserv.client.master.UserHost;
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
public class UserAcl_GetTableHandler implements TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.MASTER_HOSTS);
	}

	@Override
	public void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new UserAcl(),
			"select * from master.\"UserAcl\""
		);
	}

	@Override
	public void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new UserAcl(),
			"select distinct\n"
			+ "  mh.*\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms,\n"
			+ "  account.\"AccountHost\" bs,\n"
			+ "  billing.\"Package\" pk,\n"
			+ "  account.\"Username\" un,\n"
			+ "  master.\"UserAcl\" mh\n"
			+ "where\n"
			+ "  ms.username=?\n"
			+ "  and ms.server=bs.server\n"
			+ "  and bs.accounting=pk.accounting\n"
			+ "  and pk.name=un.package\n"
			+ "  and un.username=mh.username",
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
			new UserAcl(),
			"select\n"
			+ "  mh.*\n"
			+ "from\n"
			+ "  account.\"Username\" un1,\n"
			+ "  billing.\"Package\" pk1,\n"
			+ TableHandler.BU1_PARENTS_JOIN
			+ "  billing.\"Package\" pk2,\n"
			+ "  account.\"Username\" un2,\n"
			+ "  master.\"UserAcl\" mh\n"
			+ "where\n"
			+ "  un1.username=?\n"
			+ "  and un1.package=pk1.name\n"
			+ "  and (\n"
			+ "    un2.username=un1.username\n"
			+ TableHandler.PK1_BU1_PARENTS_OR_WHERE
			+ "  )\n"
			+ "  and bu1.accounting=pk2.accounting\n"
			+ "  and pk2.name=un2.package\n"
			+ "  and un2.username=mh.username",
			source.getUsername()
		);
	}
}