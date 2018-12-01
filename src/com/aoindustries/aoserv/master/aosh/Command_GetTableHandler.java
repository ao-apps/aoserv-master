/*
 * Copyright 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.aosh;

import com.aoindustries.aoserv.master.account.*;
import com.aoindustries.aoserv.client.account.UsState;
import com.aoindustries.aoserv.client.aosh.Command;
import com.aoindustries.aoserv.client.schema.Table;
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
public class Command_GetTableHandler implements TableHandler.GetTableHandlerPublic {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.AOSH_COMMANDS);
	}

	@Override
	public void getTablePublic(DatabaseConnection conn, RequestSource source, CompressedDataOutputStream out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			new Command(),
			"select\n"
			+ "  ac.command,\n"
			+ "  ac.\"sinceVersion\",\n"
			+ "  ac.\"lastVersion\",\n"
			+ "  st.\"name\" as \"table\",\n"
			+ "  ac.description,\n"
			+ "  ac.syntax\n"
			+ "from\n"
			+ "  \"schema\".\"AoservProtocol\" client_ap,\n"
			+ "                   aosh.\"Command\"              ac\n"
			+ "  inner join \"schema\".\"AoservProtocol\" since_ap on ac.\"sinceVersion\" = since_ap.version\n"
			+ "  left  join \"schema\".\"AoservProtocol\"  last_ap on ac.\"lastVersion\"  =  last_ap.version\n"
			+ "  left  join \"schema\".\"Table\"                st on ac.\"table\"        =       st.id\n"
			+ "where\n"
			+ "  client_ap.version=?\n"
			+ "  and client_ap.created >= since_ap.created\n"
			+ "  and (\n"
			+ "    last_ap.created is null\n"
			+ "    or client_ap.created <= last_ap.created\n"
			+ "  )",
			source.getProtocolVersion().getVersion()
		);
	}
}