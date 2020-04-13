/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2018, 2019  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of aoserv-master.
 *
 * aoserv-master is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * aoserv-master is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with aoserv-master.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.aoserv.master.email;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.email.SendmailBind;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import static com.aoindustries.aoserv.master.TableHandler.BU2_PARENTS_JOIN;
import static com.aoindustries.aoserv.master.TableHandler.PK3_BU2_PARENTS_WHERE;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.stream.StreamableOutput;
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
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new SendmailBind(),
			"select * from email.\"SendmailBind\""
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
			new SendmailBind(),
			"select\n"
			+ "  sb.*\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms\n"
			+ "  inner join net.\"Bind\" nb on ms.server=nb.server\n"
			+ "  inner join email.\"SendmailBind\" sb on nb.id=sb.net_bind\n"
			+ "where\n"
			+ "  ms.username=?",
			source.getCurrentAdministrator()
		);
	}

	@Override
	protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
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
			+ "      account.\"User\" un1,\n"
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
			+ "      account.\"User\" un2,\n"
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
			currentAdministrator,
			currentAdministrator
		);
	}
}
