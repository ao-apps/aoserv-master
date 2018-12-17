/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.scm;

import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.scm.CvsRepository;
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
public class CvsRepository_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.CVS_REPOSITORIES);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new CvsRepository(),
			"select * from scm.\"CvsRepository\""
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
			new CvsRepository(),
			"select\n"
			+ "  cr.*\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms,\n"
			+ "  linux.\"UserServer\" lsa,\n"
			+ "  scm.\"CvsRepository\" cr\n"
			+ "where\n"
			+ "  ms.username=?\n"
			+ "  and ms.server=lsa.ao_server\n"
			+ "  and lsa.id=cr.linux_server_account",
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
			new CvsRepository(),
			"select\n"
			+ "  cr.*\n"
			+ "from\n"
			+ "  account.\"User\" un1,\n"
			+ "  billing.\"Package\" pk1,\n"
			+ TableHandler.BU1_PARENTS_JOIN
			+ "  billing.\"Package\" pk2,\n"
			+ "  account.\"User\" un2,\n"
			+ "  linux.\"UserServer\" lsa,\n"
			+ "  scm.\"CvsRepository\" cr\n"
			+ "where\n"
			+ "  un1.username=?\n"
			+ "  and un1.package=pk1.name\n"
			+ "  and (\n"
			+ TableHandler.PK1_BU1_PARENTS_WHERE
			+ "  )\n"
			+ "  and bu1.accounting=pk2.accounting\n"
			+ "  and pk2.name=un2.package\n"
			+ "  and un2.username=lsa.username\n"
			+ "  and lsa.id=cr.linux_server_account",
			source.getUsername()
		);
	}
}
