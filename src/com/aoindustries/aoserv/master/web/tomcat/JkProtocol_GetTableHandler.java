/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.web.tomcat;

import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.web.tomcat.JkProtocol;
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
public class JkProtocol_GetTableHandler extends TableHandler.GetTableHandlerPublic {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.HTTPD_JK_PROTOCOLS);
	}

	@Override
	protected void getTablePublic(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.SELECT,
			new JkProtocol(),
			"select * from \"web.tomcat\".\"JkProtocol\""
		);
	}
}
