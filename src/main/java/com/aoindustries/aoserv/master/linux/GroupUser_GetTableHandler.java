/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2018, 2019, 2021  AO Industries, Inc.
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
package com.aoindustries.aoserv.master.linux;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.linux.Group;
import com.aoindustries.aoserv.client.linux.GroupUser;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class GroupUser_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.LINUX_GROUP_ACCOUNTS);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new GroupUser(),
			"select * from linux.\"GroupUser\""
		);
	}

	@Override
	protected void getTableDaemon(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new GroupUser(),
			"select\n"
			+ "  *\n"
			+ "from\n"
			+ "  linux.\"GroupUser\"\n"
			+ "where\n"
			+ "  \"group\" in (\n"
			+ "    select\n"
			+ "      lsg.name\n"
			+ "      from\n"
			+ "        master.\"UserHost\" ms1,\n"
			+ "        linux.\"GroupServer\" lsg\n"
			+ "      where\n"
			+ "        ms1.username=?\n"
			+ "        and ms1.server=lsg.ao_server\n"
			+ "  )\n"
			+ "  and \"user\" in (\n"
			+ "    select\n"
			+ "      lsa.username\n"
			+ "      from\n"
			+ "        master.\"UserHost\" ms2,\n"
			+ "        linux.\"UserServer\" lsa\n"
			+ "      where\n"
			+ "        ms2.username=?\n"
			+ "        and ms2.server=lsa.ao_server\n"
			+ "  )",
			currentAdministrator,
			currentAdministrator
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
			new GroupUser(),
			"select\n"
			+ " *\n"
			+ "from\n"
			+ "  linux.\"GroupUser\"\n"
			+ "where\n"
			+ "  \"group\" in (\n"
			+ "    select\n"
			+ "      lg.name\n"
			+ "    from\n"
			+ "      account.\"User\" un1,\n"
			+ "      billing.\"Package\" pk1,\n"
			+ TableHandler.BU1_PARENTS_JOIN
			+ "      billing.\"Package\" pk2,\n"
			+ "      linux.\"Group\" lg\n"
			+ "    where\n"
			+ "      un1.username=?\n"
			+ "      and un1.package=pk1.name\n"
			+ "      and (\n"
			+ "        lg.name in (?,?,?)\n"
			+ TableHandler.PK1_BU1_PARENTS_OR_WHERE
			+ "      )\n"
			+ "      and bu1.accounting=pk2.accounting\n"
			+ "      and pk2.name=lg.package\n"
			+ "  )\n"
			+ "  and \"user\" in (\n"
			+ "    select\n"
			+ "      la.username\n"
			+ "    from\n"
			+ "      account.\"User\" un2,\n"
			+ "      billing.\"Package\" pk3,\n"
			+ TableHandler.BU2_PARENTS_JOIN
			+ "      billing.\"Package\" pk4,\n"
			+ "      account.\"User\" un3,\n"
			+ "      linux.\"User\" la\n"
			+ "    where\n"
			+ "      un2.username=?\n"
			+ "      and un2.package=pk3.name\n"
			+ "      and (\n"
			+ "        un3.username='"+com.aoindustries.aoserv.client.linux.User.MAIL+"'\n"
			+ TableHandler.PK3_BU2_PARENTS_OR_WHERE
			+ "      )\n"
			+ "      and bu"+Account.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting=pk4.accounting\n"
			+ "      and pk4.name=un3.package\n"
			+ "      and un3.username=la.username\n"
			+ "  )",
			currentAdministrator,
			Group.FTPONLY,
			Group.MAIL,
			Group.MAILONLY,
			currentAdministrator
		);
	}
}
