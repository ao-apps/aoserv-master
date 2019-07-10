/*
 * Copyright 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.web;

import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.web.HttpdBind;
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
public class HttpdBind_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.HTTPD_BINDS);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new HttpdBind(),
			"select * from web.\"HttpdBind\""
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
			new HttpdBind(),
			"select\n"
			+ "  hb.*\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms,\n"
			+ "  net.\"Bind\" nb,\n"
			+ "  web.\"HttpdBind\" hb\n"
			+ "where\n"
			+ "  ms.username=?\n"
			+ "  and ms.server=nb.server\n"
			+ "  and nb.id=hb.net_bind",
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
			new HttpdBind(),
			"select\n"
			+ "  hb.*\n"
			+ "from\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk1,\n"
			+ TableHandler.BU1_PARENTS_JOIN
			+ "  billing.\"Package\" pk2,\n"
			+ "  web.\"Site\" hs,\n"
			+ "  web.\"VirtualHost\" hsb,\n"
			+ "  web.\"HttpdBind\" hb,\n"
			+ "  net.\"Bind\" nb\n"
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk1.name\n"
			+ "  and (\n"
			+ TableHandler.PK1_BU1_PARENTS_WHERE
			+ "  )\n"
			+ "  and bu1.accounting=pk2.accounting\n"
			+ "  and pk2.name=hs.package\n"
			+ "  and hs.id=hsb.httpd_site\n"
			+ "  and hsb.httpd_bind=hb.net_bind\n"
			+ "  and hb.net_bind=nb.id\n"
			+ "group by\n"
			+ "  hb.net_bind,\n"
			+ "  hb.httpd_server,\n"
			+ "  nb.server,\n"
			+ "  nb.\"ipAddress\",\n"
			+ "  nb.port,\n"
			+ "  nb.net_protocol",
			source.getCurrentAdministrator()
		);
	}
}
