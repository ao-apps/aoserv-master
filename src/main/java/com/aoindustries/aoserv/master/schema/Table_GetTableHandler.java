/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2018, 2019, 2020  AO Industries, Inc.
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
package com.aoindustries.aoserv.master.schema;

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
public class Table_GetTableHandler extends TableHandler.GetTableHandlerPublic {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.SCHEMA_TABLES);
	}

	@Override
	protected void getTablePublic(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.SELECT,
			new Table(),
			"select\n"
			+ "  ROW_NUMBER() OVER (ORDER BY st.id) - 1 as \"id\",\n"
			+ "  st.\"name\",\n"
			+ "  st.\"sinceVersion\",\n"
			+ "  st.\"lastVersion\",\n"
			+ "  st.display,\n"
			+ "  st.\"isPublic\",\n"
			+ "  coalesce(st.description, d.description, '') as description\n"
			+ "from\n"
			+ "  \"schema\".\"AoservProtocol\" client_ap,\n"
			+ "             \"schema\".\"Table\"                        st\n"
			+ "  inner join \"schema\".\"Schema\"                        s on st.\"schema\"       =                s.id\n"
			+ "  inner join \"schema\".\"AoservProtocol\" \"sinceVersion\" on st.\"sinceVersion\" = \"sinceVersion\".version\n"
			+ "  left  join \"schema\".\"AoservProtocol\"  \"lastVersion\" on st.\"lastVersion\"  =  \"lastVersion\".version\n"
			+ "  left  join (\n"
			+ "    select\n"
			+ "      pn.nspname, pc.relname, pd.description\n"
			+ "    from\n"
			+ "                 pg_catalog.pg_namespace   pn\n"
			+ "      inner join pg_catalog.pg_class       pc on pn.oid = pc.relnamespace\n"
			+ "      inner join pg_catalog.pg_description pd on pc.oid = pd.objoid and pd.objsubid=0\n"
			+ "  ) d on (s.\"name\", st.\"name\") = (d.nspname, d.relname)\n"
			+ "where\n"
			+ "  client_ap.version=?\n"
			+ "  and client_ap.created >= \"sinceVersion\".created\n"
			+ "  and (\"lastVersion\".created is null or client_ap.created <= \"lastVersion\".created)\n"
			// TODO: This order by will probably not be necessary once the client orders with Comparable
			+ "order by\n"
			+ "  st.id",
			source.getProtocolVersion().getVersion()
		);
		/*
		List<Table> clientTables=new ArrayList<>();
		PreparedStatement pstmt = conn.getConnection(true).prepareStatement(
			"select\n"
			+ "  st.id,\n"
			+ "  st.\"name\",\n"
			+ "  st.\"sinceVersion\",\n"
			+ "  st.\"lastVersion\",\n"
			+ "  st.display,\n"
			+ "  st.\"isPublic\",\n"
			+ "  coalesce(st.description, d.description, '') as description\n"
			+ "from\n"
			+ "  \"schema\".\"AoservProtocol\" client_ap,\n"
			+ "             \"schema\".\"Table\"                        st\n"
			+ "  inner join \"schema\".\"Schema\"                        s on st.\"schema\"       =                s.id\n"
			+ "  inner join \"schema\".\"AoservProtocol\" \"sinceVersion\" on st.\"sinceVersion\" = \"sinceVersion\".version\n"
			+ "  left  join \"schema\".\"AoservProtocol\"  \"lastVersion\" on st.\"lastVersion\"  =  \"lastVersion\".version\n"
			+ "  left  join (\n"
			+ "    select\n"
			+ "      pn.nspname, pc.relname, pd.description\n"
			+ "    from\n"
			+ "                 pg_catalog.pg_namespace   pn\n"
			+ "      inner join pg_catalog.pg_class       pc on pn.oid = pc.relnamespace\n"
			+ "      inner join pg_catalog.pg_description pd on pc.oid = pd.objoid and pd.objsubid=0\n"
			+ "  ) d on (s.\"name\", st.\"name\") = (d.nspname, d.relname)\n"
			+ "where\n"
			+ "  client_ap.version=?\n"
			+ "  and client_ap.created >= \"sinceVersion\".created\n"
			+ "  and (\"lastVersion\".created is null or client_ap.created <= \"lastVersion\".created)\n"
			+ "order by\n"
			+ "  st.id"
		);
		try {
			pstmt.setString(1, source.getProtocolVersion().getVersion());

			ResultSet results=pstmt.executeQuery();
			try {
				int clientTableID=0;
				Table tempST=new Table();
				while(results.next()) {
					tempST.init(results);
					clientTables.add(
						new Table(
							clientTableID++,
							tempST.getName(),
							tempST.getSinceVersion_version(),
							tempST.getLastVersion_version(),
							tempST.getDisplay(),
							tempST.isPublic(),
							tempST.getDescription()
						)
					);
				}
			} finally {
				results.close();
			}
		} finally {
			pstmt.close();
		}
		MasterServer.writeObjects(
			source,
			out,
			provideProgress,
			clientTables
		);
		 */
	}
}
