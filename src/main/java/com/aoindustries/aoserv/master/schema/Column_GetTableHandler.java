/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2018, 2019, 2020, 2021  AO Industries, Inc.
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

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoindustries.aoserv.client.schema.Column;
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
public class Column_GetTableHandler extends TableHandler.GetTableHandlerPublic {

	@Override
	public Set<Table.TableID> getTableIds() {
		return EnumSet.of(Table.TableID.SCHEMA_COLUMNS);
	}

	@Override
	protected void getTablePublic(DatabaseConnection conn, RequestSource source, StreamableOutput out, boolean provideProgress, Table.TableID tableID) throws IOException, SQLException {
		MasterServer.writeObjects(
			conn,
			source,
			out,
			provideProgress,
			CursorMode.SELECT,
			new Column(),
			"select\n"
			+ "  sc.id,\n"
			+ "  st.\"name\" as \"table\",\n"
			+ "  sc.\"name\",\n"
			+ "  sc.\"sinceVersion\",\n"
			+ "  sc.\"lastVersion\",\n"
			+ "  ROW_NUMBER() OVER (PARTITION BY sc.\"table\" ORDER BY sc.\"index\") - 1 as \"index\",\n"
			+ "  ty.\"name\" as \"type\",\n"
			+ "  sc.\"isNullable\",\n"
			+ "  sc.\"isUnique\",\n"
			+ "  sc.\"isPublic\",\n"
			+ "  coalesce(sc.description, d.description, '') as description\n"
			+ "from\n"
			+ "  \"schema\".\"AoservProtocol\" client_ap,\n"
			+ "             \"schema\".\"Column\"              sc\n"
			+ "  inner join \"schema\".\"Table\"               st on sc.\"table\"        =      st.id\n"
			+ "  inner join \"schema\".\"Schema\"               s on st.\"schema\"       =       s.id\n"
			+ "  inner join \"schema\".\"Type\"                ty on sc.\"type\"         =      ty.id\n"
			+ "  inner join \"schema\".\"AoservProtocol\"   sc_ap on sc.\"sinceVersion\" =   sc_ap.version\n"
			+ "  left  join \"schema\".\"AoservProtocol\" last_ap on sc.\"lastVersion\"  = last_ap.version\n"
			+ "  left  join (\n"
			+ "    select\n"
			+ "      pn.nspname, pc.relname, pa.attname, pd.description\n"
			+ "    from\n"
			+ "                 pg_catalog.pg_namespace   pn\n"
			+ "      inner join pg_catalog.pg_class       pc on pn.oid = pc.relnamespace\n"
			+ "      inner join pg_catalog.pg_attribute   pa on pc.oid = pa.attrelid\n"
			+ "      inner join pg_catalog.pg_description pd on pc.oid = pd.objoid and pd.objsubid = pa.attnum\n"
			+ "  ) d on (s.\"name\", st.\"name\", sc.\"name\") = (d.nspname, d.relname, d.attname)\n"
			+ "where\n"
			+ "  client_ap.version=?\n"
			+ "  and client_ap.created >= sc_ap.created\n"
			+ "  and (last_ap.created is null or client_ap.created <= last_ap.created)\n"
			// TODO: This order by will probably not be necessary once the client orders with Comparable
			+ "order by\n"
			+ "  st.id,\n"
			+ "  sc.index",
			source.getProtocolVersion().getVersion()
		);
		/*
		List<Column> clientColumns=new ArrayList<>();
		try (
			PreparedStatement pstmt = conn.getConnection(true).prepareStatement(
				"select\n"
				+ "  sc.id,\n"
				+ "  st.\"name\" as \"table\",\n"
				+ "  sc.\"name\",\n"
				+ "  sc.\"sinceVersion\",\n"
				+ "  sc.\"lastVersion\",\n"
				+ "  sc.index,\n"
				+ "  ty.\"name\" as \"type\",\n"
				+ "  sc.\"isNullable\",\n"
				+ "  sc.\"isUnique\",\n"
				+ "  sc.\"isPublic\",\n"
				+ "  coalesce(sc.description, d.description, '') as description\n"
				+ "from\n"
				+ "  \"schema\".\"AoservProtocol\" client_ap,\n"
				+ "             \"schema\".\"Column\"              sc\n"
				+ "  inner join \"schema\".\"Table\"               st on sc.\"table\"        =      st.id\n"
				+ "  inner join \"schema\".\"Schema\"               s on st.\"schema\"       =       s.id\n"
				+ "  inner join \"schema\".\"Type\"                ty on sc.\"type\"         =      ty.id\n"
				+ "  inner join \"schema\".\"AoservProtocol\"   sc_ap on sc.\"sinceVersion\" =   sc_ap.version\n"
				+ "  left  join \"schema\".\"AoservProtocol\" last_ap on sc.\"lastVersion\"  = last_ap.version\n"
				+ "  left  join (\n"
				+ "    select\n"
				+ "      pn.nspname, pc.relname, pa.attname, pd.description\n"
				+ "    from\n"
				+ "                 pg_catalog.pg_namespace   pn\n"
				+ "      inner join pg_catalog.pg_class       pc on pn.oid = pc.relnamespace\n"
				+ "      inner join pg_catalog.pg_attribute   pa on pc.oid = pa.attrelid\n"
				+ "      inner join pg_catalog.pg_description pd on pc.oid = pd.objoid and pd.objsubid = pa.attnum\n"
				+ "  ) d on (s.\"name\", st.\"name\", sc.\"name\") = (d.nspname, d.relname, d.attname)\n"
				+ "where\n"
				+ "  client_ap.version=?\n"
				+ "  and client_ap.created >= sc_ap.created\n"
				+ "  and (last_ap.created is null or client_ap.created <= last_ap.created)\n"
				+ "order by\n"
				+ "  st.id,\n"
				+ "  sc.index"
			)
		) {
			try {
				pstmt.setString(1, source.getProtocolVersion().getVersion());

				try (ResultSet results = pstmt.executeQuery()) {
					short clientColumnIndex = 0;
					String lastTableName=null;
					Column tempSC=new Column();
					while(results.next()) {
						tempSC.init(results);
						// Change the table ID if on next table
						String tableName = tempSC.getTable_name();
						if(lastTableName==null || !lastTableName.equals(tableName)) {
							clientColumnIndex = 0;
							lastTableName=tableName;
						}
						clientColumns.add(
							new Column(
								tempSC.getPkey(),
								tableName,
								tempSC.getName(),
								tempSC.getSinceVersion_version(),
								tempSC.getLastVersion_version(),
								clientColumnIndex++,
								tempSC.getType_name(),
								tempSC.isNullable(),
								tempSC.isUnique(),
								tempSC.isPublic(),
								tempSC.getDescription()
							)
						);
					}
				}
			} catch(Error | RuntimeException | SQLException e) {
				ErrorPrinter.addSQL(e, pstmt);
				throw e;
			}
		}
		MasterServer.writeObjects(
			source,
			out,
			provideProgress,
			clientColumns
		);
		 */
	}
}
