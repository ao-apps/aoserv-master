/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.net;

import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.net.BindFirewallZone;
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
public class BindFirewallZone_GetTableHandler implements TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.NET_BIND_FIREWALLD_ZONES);
	}

	@Override
	public void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new BindFirewallZone(),
			"select * from net.\"BindFirewallZone\""
		);
	}

	@Override
	public void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new BindFirewallZone(),
			"select\n"
			+ "  nbfz.*\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms,\n"
			+ "  net.\"Bind\" nb,\n"
			+ "  net.\"BindFirewallZone\" nbfz\n"
			+ "where\n"
			+ "  ms.username=?\n"
			+ "  and ms.server=nb.server\n"
			+ "  and nb.id=nbfz.net_bind",
			source.getUsername()
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
			new BindFirewallZone(),
			"select\n"
			+ "  nbfz.*\n"
			+ "from\n"
			+ "  net.\"Bind\" nb\n"
			+ "  inner join net.\"BindFirewallZone\" nbfz on nb.id=nbfz.net_bind\n"
			+ "where\n"
			+ "  nb.id in (\n"
			+ "    select\n"
			+ "      nb2.id\n"
			+ "    from\n"
			+ "      account.\"Username\" un1,\n"
			+ "      billing.\"Package\" pk1,\n"
			+ TableHandler.BU1_PARENTS_JOIN
			+ "      billing.\"Package\" pk2,\n"
			+ "      net.\"Bind\" nb2\n"
			+ "    where\n"
			+ "      un1.username=?\n"
			+ "      and un1.package=pk1.name\n"
			+ "      and (\n"
			+ TableHandler.PK1_BU1_PARENTS_WHERE
			+ "      )\n"
			+ "      and bu1.accounting=pk2.accounting\n"
			+ "      and pk2.name=nb2.package\n"
			+ "  )\n"
			+ "  or nb.id in (\n"
			+ "    select\n"
			+ "      nb3.id\n"
			+ "    from\n"
			+ "      account.\"Username\" un3,\n"
			+ "      billing.\"Package\" pk3,\n"
			+ TableHandler.BU2_PARENTS_JOIN
			+ "      billing.\"Package\" pk4,\n"
			+ "      web.\"Site\" hs,\n"
			+ "      web.\"VirtualHost\" hsb,\n"
			+ "      net.\"Bind\" nb3\n"
			+ "    where\n"
			+ "      un3.username=?\n"
			+ "      and un3.package=pk3.name\n"
			+ "      and (\n"
			+ TableHandler.PK3_BU2_PARENTS_WHERE
			+ "      )\n"
			+ "      and bu"+Account.MAXIMUM_BUSINESS_TREE_DEPTH+".accounting=pk4.accounting\n"
			+ "      and pk4.name=hs.package\n"
			+ "      and hs.id=hsb.httpd_site\n"
			+ "      and hsb.httpd_bind=nb3.id\n"
			+ "  ) or nb.id in (\n"
			+ "    select\n"
			+ "      ms4.bind\n"
			+ "    from\n"
			+ "      account.\"Username\" un4,\n"
			+ "      billing.\"Package\" pk4,\n"
			+ "      account.\"AccountHost\" bs4,\n"
			+ "      mysql.\"Server\" ms4\n"
			+ "    where\n"
			+ "      un4.username=?\n"
			+ "      and un4.package=pk4.name\n"
			+ "      and pk4.accounting=bs4.accounting\n"
			+ "      and bs4.server=ms4.ao_server\n"
			+ "  ) or nb.id in (\n"
			+ "    select\n"
			+ "      ps5.bind\n"
			+ "    from\n"
			+ "      account.\"Username\" un5,\n"
			+ "      billing.\"Package\" pk5,\n"
			+ "      account.\"AccountHost\" bs5,\n"
			+ "      postgresql.\"Server\" ps5\n"
			+ "    where\n"
			+ "      un5.username=?\n"
			+ "      and un5.package=pk5.name\n"
			+ "      and pk5.accounting=bs5.accounting\n"
			+ "      and bs5.server=ps5.ao_server\n"
			+ "  )",
			username,
			username,
			username,
			username
		);
	}
}
