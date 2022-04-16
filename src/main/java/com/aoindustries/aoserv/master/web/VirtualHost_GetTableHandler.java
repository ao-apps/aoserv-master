/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2018, 2019, 2021, 2022  AO Industries, Inc.
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
 * along with aoserv-master.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.aoindustries.aoserv.master.web;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.web.VirtualHost;
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
public class VirtualHost_GetTableHandler extends TableHandler.GetTableHandlerByRole {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.HTTPD_SITE_BINDS);
	}

	@Override
	protected void getTableMaster(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID, User masterUser) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new VirtualHost(),
			"select\n"
			+ "  hsb.*,\n"
			// Protocol conversion
			+ "  sc.cert_file  as ssl_cert_file,\n"
			+ "  sc.key_file   as ssl_cert_key_file,\n"
			+ "  sc.chain_file as ssl_cert_chain_file\n"
			+ "from\n"
			+ "  web.\"VirtualHost\" hsb\n"
			// Protocol conversion
			+ "  left join pki.\"Certificate\" sc on hsb.certificate=sc.id"
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
			new VirtualHost(),
			"select\n"
			+ "  hsb.*,\n"
			// Protocol conversion
			+ "  sc.cert_file  as ssl_cert_file,\n"
			+ "  sc.key_file   as ssl_cert_key_file,\n"
			+ "  sc.chain_file as ssl_cert_chain_file\n"
			+ "from\n"
			+ "  master.\"UserHost\" ms,\n"
			+ "  web.\"Site\" hs,\n"
			+ "  web.\"VirtualHost\" hsb\n"
			// Protocol conversion
			+ "  left join pki.\"Certificate\" sc on hsb.certificate=sc.id\n"
			+ "where\n"
			+ "  ms.username=?\n"
			+ "  and ms.server=hs.ao_server\n"
			+ "  and hs.id=hsb.httpd_site",
			source.getCurrentAdministrator()
		);
	}

	@Override
	protected void getTableAdministrator(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.AUTO,
			new VirtualHost(),
			"select\n"
			+ "  hsb.*,\n"
			// Protocol conversion
			+ "  sc.cert_file  as ssl_cert_file,\n"
			+ "  sc.key_file   as ssl_cert_key_file,\n"
			+ "  sc.chain_file as ssl_cert_chain_file\n"
			+ "from\n"
			+ "  account.\"User\" un,\n"
			+ "  billing.\"Package\" pk1,\n"
			+ TableHandler.BU1_PARENTS_JOIN
			+ "  billing.\"Package\" pk2,\n"
			+ "  web.\"Site\" hs,\n"
			+ "  web.\"VirtualHost\" hsb\n"
			// Protocol conversion
			+ "  left join pki.\"Certificate\" sc on hsb.certificate=sc.id\n"
			+ "where\n"
			+ "  un.username=?\n"
			+ "  and un.package=pk1.name\n"
			+ "  and (\n"
			+ TableHandler.PK1_BU1_PARENTS_WHERE
			+ "  )\n"
			+ "  and bu1.accounting=pk2.accounting\n"
			+ "  and pk2.name=hs.package\n"
			+ "  and hs.id=hsb.httpd_site",
			source.getCurrentAdministrator()
		);
	}
}
