/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.email;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.email.SendmailBind;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import static com.aoindustries.aoserv.master.TableHandler.BU2_PARENTS_JOIN;
import static com.aoindustries.aoserv.master.TableHandler.PK3_BU2_PARENTS_WHERE;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.CompressedDataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class SendmailBind_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.SENDMAIL_BINDS);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new SendmailBind(),
			"select * from email.\"SendmailBind\""
		);
	}

	@Override
	protected void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new SendmailBind(),
			"select\n"
			+ "  sb.*\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms\n"
			+ "  inner join net.\"Bind\" nb on ms.server=nb.server\n"
			+ "  inner join email.\"SendmailBind\" sb on nb.id=sb.net_bind\n"
			+ "where\n"
			+ "  ms.username=?",
			source.getUsername()
		);
	}

	@Override
	protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		UserId username = source.getUsername();
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new SendmailBind(),
			"select\n"
			+ "  *\n"
			+ "from\n"
			+ "  email.\"SendmailBind\"\n"
			+ "where\n"
			// Allow by matching net.Bind.package
			+ "  net_bind in (\n"
			+ "    select\n"
			+ "      nb.id\n"
			+ "    from\n"
			+ "      account.\"Username\" un1,\n"
			+ "      billing.\"Package\" pk1,\n"
			+ TableHandler.BU1_PARENTS_JOIN
			+ "      billing.\"Package\" pk2,\n"
			+ "      net.\"Bind\" nb\n"
			+ "    where\n"
			+ "      un1.username=?\n"
			+ "      and un1.package=pk1.name\n"
			+ "      and (\n"
			+ TableHandler.PK1_BU1_PARENTS_WHERE
			+ "      )\n"
			+ "      and bu1.accounting=pk2.accounting\n"
			+ "      and pk2.name=nb.package\n"
			+ "  )\n"
			// Allow by matching email.SendmailServer.package
			+ "  or sendmail_server in (\n"
			+ "    select\n"
			+ "      ss.id\n"
			+ "    from\n"
			+ "      account.\"Username\" un2,\n"
			+ "      billing.\"Package\" pk3,\n"
			+ BU2_PARENTS_JOIN
			+ "      billing.\"Package\" pk4,\n"
			+ "      email.\"SendmailServer\" ss\n"
			+ "    where\n"
			+ "      un2.username=?\n"
			+ "      and un2.package=pk3.name\n"
			+ "      and (\n"
			+ PK3_BU2_PARENTS_WHERE
			+ "      )\n"
			+ "      and bu"+Account.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting=pk4.accounting\n"
			+ "      and pk4.id=ss.package\n"
			+ "  )",
			username,
			username
		);
	}
}
