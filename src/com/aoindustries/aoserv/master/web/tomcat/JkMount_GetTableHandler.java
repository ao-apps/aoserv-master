/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.web.tomcat;

import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.web.tomcat.JkMount;
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
public class JkMount_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.HTTPD_TOMCAT_SITE_JK_MOUNTS);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new JkMount(),
			"select * from \"web.tomcat\".\"JkMount\""
		);
	}

	@Override
	protected void getTableDaemon(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID, User masterUser, UserHost[] masterServers) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new JkMount(),
			"select\n"
			+ "  htsjm.*\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms,\n"
			+ "  web.\"Site\" hs,\n"
			+ "  \"web.tomcat\".\"JkMount\" htsjm\n"
			+ "where\n"
			+ "  ms.username=?\n"
			+ "  and ms.server=hs.ao_server\n"
			+ "  and hs.id=htsjm.httpd_tomcat_site",
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
			new JkMount(),
			"select\n"
			+ "  htsjm.*\n"
			+ "from\n"
			+ "  account.\"Username\" un,\n"
			+ "  billing.\"Package\" pk1,\n"
			+ TableHandler.BU1_PARENTS_JOIN
			+ "  billing.\"Package\" pk2,\n"
			+ "  web.\"Site\" hs,\n"
			+ "  \"web.tomcat\".\"JkMount\" htsjm\n"
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk1.name\n"
			+ "  and (\n"
			+ TableHandler.PK1_BU1_PARENTS_WHERE
			+ "  )\n"
			+ "  and bu1.accounting=pk2.accounting\n"
			+ "  and pk2.name=hs.package\n"
			+ "  and hs.id=htsjm.httpd_tomcat_site",
			source.getUsername()
		);
	}
}