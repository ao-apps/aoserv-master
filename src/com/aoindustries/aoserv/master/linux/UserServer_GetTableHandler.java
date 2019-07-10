/*
 * Copyright 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.linux;

import com.aoindustries.aoserv.client.linux.UserServer;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.AoservProtocol;
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
public class UserServer_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.LINUX_SERVER_ACCOUNTS);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new UserServer(),
			"select * from linux.\"UserServer\""
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
			new UserServer(),
			"select distinct\n"
			+ "  lsa.*\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms\n"
			+ "  left join linux.\"Server\" ff on ms.server=ff.failover_server,\n"
			+ "  linux.\"UserServer\" lsa\n"
			+ "where\n"
			+ "  ms.username=?\n"
			+ "  and (\n"
			+ "    ms.server=lsa.ao_server\n"
			+ "    or ff.server=lsa.ao_server\n"
			+ "  )",
			source.getCurrentAdministrator()
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
			new UserServer(),
			"select\n"
			+ "  lsa.id,\n"
			+ "  lsa.username,\n"
			+ "  lsa.ao_server,\n"
			+ "  lsa.uid,\n"
			+ "  lsa.home,\n"
			+ "  lsa.autoresponder_from,\n"
			+ "  lsa.autoresponder_subject,\n"
			+ "  lsa.autoresponder_path,\n"
			+ "  lsa.is_autoresponder_enabled,\n"
			+ "  lsa.disable_log,\n"
			+ "  case when lsa.predisable_password is null then null else ? end,\n"
			+ "  lsa.created,\n"
			+ "  lsa.use_inbox,\n"
			+ "  lsa.trash_email_retention,\n"
			+ "  lsa.junk_email_retention,\n"
			+ "  lsa.sa_integration_mode,\n"
			+ "  lsa.sa_required_score,\n"
			+ "  lsa.sa_discard_score,\n"
			+ "  lsa.sudo\n"
			+ "from\n"
			+ "  account.\"User\" un1,\n"
			+ "  billing.\"Package\" pk1,\n"
			+ TableHandler.BU1_PARENTS_JOIN
			+ "  billing.\"Package\" pk2,\n"
			+ "  account.\"User\" un2,\n"
			+ "  account.\"AccountHost\" bs,\n"
			+ "  linux.\"UserServer\" lsa\n"
			+ "where\n"
			+ "  un1.username=?\n"
			+ "  and un1.package=pk1.name\n"
			+ "  and (\n"
			+ "    un2.username=?\n"
			+ TableHandler.PK1_BU1_PARENTS_OR_WHERE
			+ "  )\n"
			+ "  and bu1.accounting=pk2.accounting\n"
			+ "  and pk2.name=un2.package\n"
			+ "  and pk1.accounting=bs.accounting\n"
			+ "  and un2.username=lsa.username\n"
			+ "  and bs.server=lsa.ao_server",
			AoservProtocol.FILTERED,
			source.getCurrentAdministrator(),
			com.aoindustries.aoserv.client.linux.User.MAIL
		);
	}
}
