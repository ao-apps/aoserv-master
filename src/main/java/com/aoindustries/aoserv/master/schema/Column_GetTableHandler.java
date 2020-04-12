/*
 * Copyright 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.schema;

import com.aoindustries.aoserv.client.schema.Column;
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
		PreparedStatement pstmt=conn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(
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
		);
		try {
			pstmt.setString(1, source.getProtocolVersion().getVersion());

			ResultSet results=pstmt.executeQuery();
			try {
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
			clientColumns
		);
		 */
	}
}
