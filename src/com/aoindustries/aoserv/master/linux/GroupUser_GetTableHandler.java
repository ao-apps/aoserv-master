/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.linux;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.linux.Group;
import com.aoindustries.aoserv.client.linux.GroupUser;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.validator.UserId;
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
public class GroupUser_GetTableHandler implements TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.LINUX_GROUP_ACCOUNTS);
	}

	@Override
	public void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new GroupUser(),
			"select * from linux.\"GroupUser\""
		);
	}

	@Override
	public void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		UserId username = source.getUsername();
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
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
			username,
			username
		);
	}

	@Override
	public void getTableAdministrator(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		UserId username = source.getUsername();
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
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
			+ "      account.\"Username\" un1,\n"
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
			+ "      account.\"Username\" un2,\n"
			+ "      billing.\"Package\" pk3,\n"
			+ TableHandler.BU2_PARENTS_JOIN
			+ "      billing.\"Package\" pk4,\n"
			+ "      account.\"Username\" un3,\n"
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
			username,
			Group.FTPONLY,
			Group.MAIL,
			Group.MAILONLY,
			username
		);
	}
}
