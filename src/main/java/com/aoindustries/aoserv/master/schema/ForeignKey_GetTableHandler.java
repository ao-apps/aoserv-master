/*
 * Copyright 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.schema;

import com.aoindustries.aoserv.client.schema.ForeignKey;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.master.CursorMode;
import com.aoindustries.aoserv.master.MasterServer;
import com.aoindustries.aoserv.master.RequestSource;
import com.aoindustries.aoserv.master.TableHandler;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.stream.StreamableOutput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Set;

/**
 * @author  AO Industries, Inc.
 */
public class ForeignKey_GetTableHandler extends TableHandler.GetTableHandlerPublic {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.SCHEMA_FOREIGN_KEYS);
	}

	@Override
	protected void getTablePublic(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.SELECT,
			new ForeignKey(),
			"select\n"
			+ "  sfk.*\n"
			+ "from\n"
			+ "  \"schema\".\"AoservProtocol\" client_ap,\n"
			+ "  \"schema\".\"ForeignKey\" sfk\n"
			+ "  inner join \"schema\".\"AoservProtocol\" \"sinceVersion\" on sfk.\"sinceVersion\" = \"sinceVersion\".version\n"
			+ "  left  join \"schema\".\"AoservProtocol\" \"lastVersion\"  on sfk.\"lastVersion\"  =  \"lastVersion\".version\n"
			+ "where\n"
			+ "  client_ap.version=?\n"
			+ "  and client_ap.created >= \"sinceVersion\".created\n"
			+ "  and (\"lastVersion\".created is null or client_ap.created <= \"lastVersion\".created)",
			source.getProtocolVersion().getVersion()
		);
	}
}